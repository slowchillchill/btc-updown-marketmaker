use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InventoryBookV1 {
    pub bid: Decimal,
    pub ask: Decimal,
    pub tick: Decimal,
}
pub(super) fn book(
    asset: &AssetBusinessObservationV1,
) -> Result<Option<InventoryBookV1>, StrategyErrorV1> {
    let best = |fact: &ObservedFactV1<Vec<PriceLevelV1>>,
                bid: bool|
     -> Result<Option<Decimal>, StrategyErrorV1> {
        let Some(levels) = value(fact) else {
            return Ok(None);
        };
        let mut best = None;
        for level in levels {
            if decimal(&level.quantity_shares)? > Decimal::ZERO {
                let price = decimal(&level.price_pusd_per_share)?;
                best = Some(best.map_or(
                    price,
                    |old: Decimal| if bid { old.max(price) } else { old.min(price) },
                ));
            }
        }
        Ok(best)
    };
    let (Some(bid), Some(ask), Some(tick)) = (
        best(&asset.bids, true)?,
        best(&asset.asks, false)?,
        value(&asset.tick),
    ) else {
        return Ok(None);
    };
    let tick = decimal(tick)?;
    Ok(
        (bid > Decimal::ZERO && bid < ask && ask < Decimal::ONE && tick > Decimal::ZERO)
            .then_some(InventoryBookV1 { bid, ask, tick }),
    )
}
pub(super) fn floor_price(
    raw: &R,
    book: &InventoryBookV1,
) -> Result<Option<Decimal>, StrategyErrorV1> {
    let capped = raw
        .clone()
        .min(r(book.ask - book.tick))
        .min(r(Decimal::ONE - book.tick));
    if !capped.is_positive() {
        return Ok(None);
    }
    let units = capped.divided_by(&r(book.tick));
    let index = (units.numerator() / units.denominator())
        .to_u16()
        .ok_or(StrategyErrorV1::Arithmetic("价格索引溢出"))?;
    let price = book.tick * Decimal::from(index);
    Ok((price > Decimal::ZERO && price < Decimal::ONE).then_some(price))
}
pub(super) fn targets(
    books: &[InventoryBookV1; 2],
    difference: Decimal,
    capacity: Decimal,
    skew_ticks: Decimal,
) -> (R, R, R, [R; 2]) {
    let two = r(Decimal::from(2));
    let c = r(books[0].bid + books[0].ask + Decimal::from(2) - books[1].bid - books[1].ask)
        .divided_by(&r(Decimal::from(4)));
    let delta = books[0].tick.max(books[1].tick);
    let h = r(books[0].ask - books[0].bid + books[1].ask - books[1].bid)
        .divided_by(&r(Decimal::from(4)))
        .max(r(delta).divided_by(&two));
    let skew = r(delta)
        .mul(&r(skew_ticks))
        .mul(&r(difference))
        .divided_by(&r(capacity));
    let raw = [
        c.sub(&h).sub(&skew),
        r(Decimal::ONE).sub(&c).sub(&h).add(&skew),
    ];
    (c, h, skew, raw)
}
pub(super) fn centishares(q: Decimal) -> Result<u128, StrategyErrorV1> {
    (q.max(Decimal::ZERO) * Decimal::from(100))
        .floor()
        .to_u128()
        .ok_or(StrategyErrorV1::Arithmetic("份额轴不能编码"))
}
pub(super) fn shares(n: u128) -> Result<Decimal, StrategyErrorV1> {
    Decimal::try_from_i128_with_scale(
        i128::try_from(n).map_err(|_| StrategyErrorV1::Arithmetic("份额轴溢出"))?,
        2,
    )
    .map_err(|_| StrategyErrorV1::Arithmetic("份额不能编码"))
}
pub(super) fn maximum(
    mut high: u128,
    mut feasible: impl FnMut(u128) -> Result<bool, StrategyErrorV1>,
) -> Result<u128, StrategyErrorV1> {
    let mut low = 0;
    while low < high {
        let middle = low + (high - low) / 2 + (high - low) % 2;
        if feasible(middle)? {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(low)
}
/// 两个有限有序断点序列的最高可行向量。量纲沿正式 0.01 份，无浮点 λ。
pub(super) fn highest_vector(
    a: [u128; 2],
    mut feasible: impl FnMut([u128; 2]) -> Result<bool, StrategyErrorV1>,
) -> Result<[u128; 2], StrategyErrorV1> {
    let mut answer = [0, 0];
    for axis in 0..2 {
        if a[axis] == 0 {
            continue;
        }
        let vector = |k: u128| -> Result<[u128; 2], StrategyErrorV1> {
            let other = (BigUint::from(k) * BigUint::from(a[1 - axis]) / BigUint::from(a[axis]))
                .to_u128()
                .ok_or(StrategyErrorV1::Arithmetic("离散份额不能编码"))?;
            let mut q = [0, 0];
            q[axis] = k;
            q[1 - axis] = other;
            Ok(q)
        };
        let k = maximum(a[axis], |k| feasible(vector(k)?))?;
        let candidate = vector(k)?;
        if candidate[0] >= answer[0] && candidate[1] >= answer[1] {
            answer = candidate;
        }
    }
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }
    fn quotes(b: [(&str, &str); 2], difference: i64) -> [Option<Decimal>; 2] {
        let books = b.map(|(bid, ask)| InventoryBookV1 {
            bid: d(bid),
            ask: d(ask),
            tick: d("0.01"),
        });
        let (_, _, _, raw) = targets(
            &books,
            Decimal::from(difference),
            Decimal::from(50),
            Decimal::ONE,
        );
        [
            floor_price(&raw[0], &books[0]).unwrap(),
            floor_price(&raw[1], &books[1]).unwrap(),
        ]
    }
    #[test]
    fn manual_prices_and_nonpositive_leg_never_clamped_up() {
        assert_eq!(
            quotes([("0.59", "0.60"), ("0.40", "0.41")], 0),
            [Some(d("0.59")), Some(d("0.40"))]
        );
        assert_eq!(
            quotes([("0.59", "0.60"), ("0.40", "0.41")], 50),
            [Some(d("0.58")), Some(d("0.40"))]
        );
        assert_eq!(
            quotes([("0.59", "0.60"), ("0.40", "0.41")], -50),
            [Some(d("0.59")), Some(d("0.39"))]
        );
        assert_eq!(
            quotes([("0.20", "0.22"), ("0.78", "0.80")], 0),
            [Some(d("0.20")), Some(d("0.78"))]
        );
        assert_eq!(
            quotes([("0.01", "0.02"), ("0.98", "0.99")], 100),
            [None, Some(d("0.98"))]
        );
    }
    #[test]
    fn highest_legal_vector_exact_cash_and_unequal_breakpoints() {
        let q = highest_vector([5000, 5000], |q| {
            let cost = r(shares(q[0]).unwrap() * d("0.59") + shares(q[1]).unwrap() * d("0.40"));
            let future =
                r(shares(q[0]).unwrap() * d("0.41")).max(r(shares(q[1]).unwrap() * d("0.60")));
            Ok(cost.add(&future) <= r(d("39.75")))
        })
        .unwrap();
        assert_eq!(q, [2500, 2500]);
        // a=(0.03,0.07),预算0.025，成本每份0.5。断点最高向量为(0.01,0.04)。
        assert_eq!(
            highest_vector([3, 7], |q| Ok(q[0] + q[1] <= 5)).unwrap(),
            [1, 4]
        );
        assert_eq!(
            highest_vector([0, 5000], |q| Ok(q[1] <= 2000)).unwrap(),
            [0, 2000]
        );
    }
}

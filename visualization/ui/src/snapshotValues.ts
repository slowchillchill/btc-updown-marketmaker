import { at, type Json } from "./facts";

// 显示舍入使用整数商和余数；原分数不变，绝不经 JS 浮点数。
export type Fraction = { numerator: bigint; denominator: bigint };
export function fraction(value: Json | undefined): Fraction | undefined {
  const v = at(value, "Known") ?? value;
  const n = at(v, "numerator"),
    d = at(v, "denominator");
  if (
    typeof n !== "string" ||
    typeof d !== "string" ||
    !/^-?\d+$/.test(n) ||
    !/^\d+$/.test(d) ||
    BigInt(d) === 0n
  )
    return undefined;
  return { numerator: BigInt(n), denominator: BigInt(d) };
}
export function decimalFraction(value: Json | undefined): Fraction | undefined {
  if (typeof value !== "string" || !/^-?\d+(\.\d+)?$/.test(value))
    return undefined;
  const [whole, rest = ""] = value.split(".");
  return {
    numerator: BigInt(whole + rest),
    denominator: 10n ** BigInt(rest.length),
  };
}
export function rounded(v: Fraction, places = 2, multiplier = 1n): string {
  const n = v.numerator * multiplier;
  const abs = n < 0n ? -n : n;
  const scale = 10n ** BigInt(places);
  const scaled = abs * scale;
  const amount =
    scaled / v.denominator +
    ((scaled % v.denominator) * 2n >= v.denominator ? 1n : 0n);
  const digits = amount.toString().padStart(places + 1, "0");
  return (
    (n < 0n && amount !== 0n ? "-" : "") +
    (places ? digits.slice(0, -places) + "." + digits.slice(-places) : digits)
  );
}
export function navChange(
  nav: Json | undefined,
  initial: Json | undefined,
): { amount: Fraction; percent?: Fraction } | undefined {
  const n = fraction(nav),
    i = decimalFraction(initial);
  if (!n || !i) return undefined;
  const amount = {
    numerator: n.numerator * i.denominator - i.numerator * n.denominator,
    denominator: n.denominator * i.denominator,
  };
  return {
    amount,
    percent:
      i.numerator > 0n
        ? {
            numerator: amount.numerator * i.denominator * 100n,
            denominator: amount.denominator * i.numerator,
          }
        : undefined,
  };
}
export function utc(value: Json | undefined): string | undefined {
  if (typeof value !== "string" || !/^\d+$/.test(value)) return undefined;
  const n = BigInt(value);
  // 只在 Date 自身合法范围内转换，用于标签；原 UTC ms 仍可点击。
  if (n > 8640000000000000n) return undefined;
  return new Date(Number(n))
    .toISOString()
    .replace("T", " ")
    .replace("Z", " UTC");
}

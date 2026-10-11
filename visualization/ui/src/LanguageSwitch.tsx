import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { languageStorageKey, type Language } from "./i18n";

export function LanguageSwitch() {
  const { t, i18n } = useTranslation();
  const language = i18n.resolvedLanguage === "en" ? "en" : "zh-CN";
  useEffect(() => {
    document.documentElement.lang = language;
    document.title = t("公共业务事实浏览器");
  }, [language, t]);

  function changeLanguage(value: Language) {
    void i18n.changeLanguage(value);
    try {
      localStorage.setItem(languageStorageKey, value);
    } catch {
      // 存储不可用不阻止当前页面的语言选择。
    }
  }

  return (
    <label className="language-switch">
      <span>{t("界面语言")}</span>
      <select
        aria-label={t("界面语言")}
        value={language}
        onChange={(event) => changeLanguage(event.target.value as Language)}
      >
        <option value="zh-CN" lang="zh-CN">
          中文
        </option>
        <option value="en" lang="en">
          English
        </option>
      </select>
    </label>
  );
}

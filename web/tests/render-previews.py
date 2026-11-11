"""Render actual UI DOM captures as explicitly labeled static previews.

Requires development-only weasyprint and beautifulsoup4. These previews do not
claim browser rendering or browser interaction QA. DOM captures come from
ui.test.cjs exercising the real in-process API with an isolated fixture DB.
"""
from pathlib import Path
import re
import sys
from bs4 import BeautifulSoup
from weasyprint import HTML
import tinycss2

ROOT = Path(__file__).resolve().parents[2]


def applicable_css(css, width):
    """Select the source's exact width rules for the static print renderer."""
    output = []
    for rule in tinycss2.parse_stylesheet(css, skip_whitespace=True):
        if rule.type == "at-rule" and rule.lower_at_keyword == "media":
            condition = tinycss2.serialize(rule.prelude)
            maximum = re.search(r"max-width:\s*(\d+)px", condition)
            minimum = re.search(r"min-width:\s*(\d+)px", condition)
            if maximum and width <= int(maximum[1]) or minimum and width >= int(minimum[1]):
                output.append(tinycss2.serialize(rule.content))
        else:
            output.append(tinycss2.serialize([rule]))
    return "\n".join(output)


def render(name, stage, width=1440, height=1100, modal=False, evidence="DEMO DATA"):
    soup = BeautifulSoup((ROOT / "screenshots" / f"{name}.dom.html").read_text(), "html.parser")
    for el in soup.select("script, link[rel='stylesheet']"):
        el.decompose()
    style = soup.new_tag("style")
    style.string = applicable_css((ROOT / "web/styles.css").read_text(), width)
    soup.head.append(style)
    disclosure = soup.new_tag("div", attrs={"class": "render-disclosure"})
    disclosure.string = f"STAGE {stage} · STATIC HTML/CSS PREVIEW · {evidence} · NOT BROWSER QA"
    soup.body.insert(0, disclosure)
    # Replace native controls with their actual current visible values for print.
    for el in soup.select("input, textarea, select"):
        replacement = soup.new_tag("div", attrs={"class": "static-control"})
        if el.name == "select":
            selected = el.select_one("option[selected]") or el.select_one("option")
            replacement.string = selected.get_text() if selected else ""
        elif el.name == "textarea":
            replacement.string = el.get_text()
        elif el.get("type") == "checkbox":
            replacement.string = "☑" if el.has_attr("checked") else "☐"
        else:
            replacement.string = el.get("value") or el.get("placeholder") or ""
        el.replace_with(replacement)
    overrides = soup.new_tag("style")
    overrides.string = f"""
    @page{{size:{width}px {height}px;margin:0}}
    html,body{{width:{width}px}}body{{font-family:'Open Sans',sans-serif}}
    .render-disclosure{{background:#f8e6b7;color:#69501e;font-size:9px;letter-spacing:.8px;padding:9px 12px;text-align:center;min-height:32px}}
    .sidebar{{top:32px;height:{height-32}px;width:226px}}
    .app-shell{{margin-left:226px}}main{{min-height:0}}
    .sidebar-footer{{position:absolute;bottom:26px;left:17px;right:17px}}
    .app-footer{{position:absolute;bottom:5px;left:226px;right:0}}
    dialog,.toast,.skip-link{{display:none}}.page-enter{{animation:none}}
    svg{{display:inline-block}}button{{appearance:none}}
    .source-chip,.legend span,.badge{{white-space:nowrap}}
    .capacity-blocks{{grid-template-columns:repeat(18,1fr)}}
    .static-control{{border:1px solid #dce5d4;padding:10px;border-radius:7px;background:white;font-size:12px;min-height:39px}}
    .search-field .static-control{{padding-left:36px;color:#829180;font-size:11px}}
    .field .static-control{{width:100%}}
    /* WeasyPrint does not implement auto-fit tracks or SVG grid row spanning. */
    .capability-grid{{grid-template-columns:repeat(3,minmax(0,1fr))}}
    .readiness-card{{display:block}}
    .readiness-card>span,.readiness-card strong,.readiness-card small{{display:block;margin-top:5px}}
    .readiness-card svg{{display:inline-block;margin-bottom:8px}}
    .breadcrumb strong,.section-title h2{{white-space:nowrap}}
    """
    if modal:
        overrides.string += f"""
        .app-shell,.sidebar{{opacity:.22}}
        dialog[open]{{display:block;position:fixed;top:95px;left:{(width-650)/2}px;width:650px;max-height:none;z-index:50;margin:0;background:white;border:1px solid #cddbc8;border-radius:16px;box-shadow:0 25px 80px #17372330}}
        .dialog-header,.dialog-body,.dialog-footer{{display:block}}
        .dialog-header{{position:relative;padding-right:50px}}
        .dialog-header .icon-button{{position:absolute;right:20px;top:22px}}
        .dialog-footer{{text-align:right}}.dialog-footer .button{{display:inline-block;margin-left:8px}}
        """
    if width < 680:
        overrides.string += f"""
        .sidebar{{display:none}}.app-shell{{margin-left:0}}.topbar{{position:static;display:flex}}
        main{{padding:23px 17px 20px}}.app-footer{{position:static;left:0}}
        .metrics{{grid-template-columns:repeat(2,1fr)}}.metric{{padding:13px}}
        .metric-label{{font-size:9px}}.metric-bottom{{font-size:8px}}.metric-value{{font-size:27px}}
        .pool-grid,.dashboard-grid,.capability-grid{{grid-template-columns:1fr}}
        .capacity-blocks{{grid-template-columns:repeat(12,1fr)}}
        .heading-actions{{display:flex;margin-top:16px}}.page-heading{{display:block}}
        .notice{{font-size:10px}}.source-chip{{font-size:8px}}
        .card-header{{gap:8px}}.card-header h2{{font-size:14px}}
        .capacity-head{{margin:12px 0}}.capacity-number{{font-size:23px}}
        .capacity-sub{{font-size:9px}}.legend{{font-size:8px;gap:10px}}
        .capacity-footer{{font-size:8px;gap:8px}}.capacity-footer .button{{font-size:8px}}
        """
    soup.head.append(overrides)
    target = ROOT / "screenshots" / (name + ("-mobile" if width < 680 else ""))
    target.with_suffix(".html").write_text(str(soup))
    HTML(string=str(soup), base_url=str(ROOT / "web")).write_pdf(target.with_suffix(".pdf"))
    print(target.with_suffix(".pdf"))


if __name__ == "__main__":
    render("stage-2-overview", 2, height=1300)
    render("stage-2-inventory", 2, height=1000)
    render("stage-3-advisory", 3, height=1100, modal=True)
    render("stage-2-overview", 4, width=390, height=1800)
    if (ROOT / 'screenshots/stage-5-cluster-control.dom.html').exists():
        render('stage-5-cluster-control', 5, height=1750)
    if (ROOT / 'screenshots/stage-6-ray-plan.dom.html').exists():
        render('stage-6-ray-plan', 6, height=1100, modal=True)
    if (ROOT / 'screenshots/stage-7-integrations.dom.html').exists():
        render('stage-7-integrations', 7, height=1700)
    if (ROOT / 'screenshots/stage-5-cluster-control.dom.html').exists():
        render('stage-5-cluster-control', 7, width=390, height=2700)
    if (ROOT / 'screenshots/stage-8-control-safety.dom.html').exists():
        render('stage-8-control-safety', 8, height=1900)

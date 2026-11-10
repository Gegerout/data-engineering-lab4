"""Снимки выводов команд и журналов координаторов для отчета, текст без заголовков и подписей."""
import html

from playwright.sync_api import sync_playwright

from common import LAB

CHROMIUM = '/opt/homebrew/bin/chromium'
LOGS = ['01_setup.txt', '02_roles.txt', '03_quorum.txt', '04_ephemeral_window1.txt', '04_ephemeral_window2.txt',
        'stage3_nofence/scenario.txt', 'stage3_nofence/coord-a.log', 'stage3_nofence/coord-b.log',
        'stage4_fence/scenario.txt', 'stage4_fence/coord-a.log', 'stage4_fence/coord-b.log']
SHOTS = LAB / 'screenshots'
PART = 30  # строк лога на один снимок, длинный вывод идет несколькими снимками подряд


def main():
    SHOTS.mkdir(exist_ok=True)
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=CHROMIUM, headless=True)
        page = b.new_page(viewport={'width': 1280, 'height': 900}, device_scale_factor=2)
        for name in LOGS:
            lines = (LAB / 'logs' / name).read_text().rstrip('\n').split('\n')
            parts = [lines[i:i + PART] for i in range(0, len(lines), PART)]
            for n, part in enumerate(parts, 1):
                text = '\n'.join(part)
                page.set_content('<!doctype html><html><meta charset="utf-8"><style>body{margin:0;background:#fff}'
                                 'pre{margin:0;padding:12px;width:820px;box-sizing:border-box;color:#111;'
                                 'font:14px/1.45 Menlo,monospace;white-space:pre-wrap;overflow-wrap:anywhere}</style>'
                                 f'<pre>{html.escape(text)}</pre></html>')
                if page.inner_text('pre').strip() != text.strip():
                    raise SystemExit(f'text mismatch: {name}')
                stem = name.replace('/', '_').rsplit('.', 1)[0] + (f'_{n}' if len(parts) > 1 else '')
                page.locator('pre').screenshot(path=str(SHOTS / f'{stem}.png'))
                print(f'{stem}.png saved, lines {len(part)}, text equals log')
        b.close()


if __name__ == '__main__':
    main()

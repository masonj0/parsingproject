#!/usr/bin/env python3
"""
Paddock Parser Toolkit - Integrated Collector Generator (v1.4)

This module generates our interactive data collection dashboard. It now reads
the "enabled" flag from config.json to dynamically show or hide data sources,
giving the user full control over the collector's content.
"""

import webbrowser
from pathlib import Path
from typing import Dict
from datetime import date
from urllib.parse import quote
import sys
import json

def load_config():
    """Loads and returns the configuration from config.json."""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: config.json not found. Please ensure it's in the project root.", file=sys.stderr)
        return None
    except json.JSONDecodeError:
        print("Error: Could not decode config.json. Please check file for syntax errors.", file=sys.stderr)
        return None

def create_and_launch_link_helper(config: Dict):
    """
    Generates and opens the interactive `collector.html` dashboard.
    This page now only displays sources that are marked as "enabled": true.
    """
    print("Generating the Integrated Collector dashboard...")

    output_dir = Path(config.get("DEFAULT_OUTPUT_DIR", "output"))
    output_dir.mkdir(exist_ok=True, parents=True)
    helper_path = output_dir / "collector.html"
    
    today = date.today()
    date_str_iso = today.strftime("%Y-%m-%d")
    
    proxy_viewers = config.get("PROXY_VIEWERS", [])
    source_categories = config.get("DATA_SOURCES", [])
    
    # --- Dynamic HTML Generation ---
    sections_html = ""
    for category in source_categories:
        title = category.get("title", "Unknown Category")
        
        # --- UPGRADED LOGIC: Filter sites based on the "enabled" flag ---
        sites = [site for site in category.get("sites", []) if site.get("enabled", True)]
        
        # If after filtering, there are no sites in this category, skip it entirely
        if not sites:
            continue

        sites_html = ""
        for site in sites:
            name = site.get("name", "Unnamed Link")
            url = site.get("url", "#").format(date_str_iso=date_str_iso)
            source_id = name.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
            
            proxy_links_html = ""
            for viewer in proxy_viewers:
                if viewer.get("ENABLED", False):
                    proxy_url_template = viewer.get("TOOL_URL", "")
                    proxy_link_text = viewer.get("LINK_TEXT", "View via Proxy")
                    if proxy_url_template:
                        encoded_url = quote(url, safe=':/')
                        proxy_full_url = proxy_url_template.format(target_url=encoded_url)
                        proxy_links_html += f' | <a href="{proxy_full_url}" target="_blank">{proxy_link_text}</a>'

            sites_html += f"""
            <div class="source-item">
                <div class="source-header">
                    <strong>{name}:</strong>
                    <div class="links">
                        <a href="{url}" target="_blank">Direct Link</a>{proxy_links_html}
                    </div>
                </div>
                <textarea id="{source_id}" placeholder="Paste source code for {name} here..."></textarea>
            </div>
            """
        sections_html += f"<h2>{title}</h2>\n<div class='source-grid'>{sites_html}</div>\n"
        
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Paddock Parser - Integrated Collector</title>
    <style>
        body {{ font-family: system-ui, sans-serif; margin: 0; padding: 25px; background: #f8f9fa; }}
        .container {{ max-width: 1200px; margin: auto; background: #fff; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
        h1, h2 {{ color: #2c3e50; border-bottom: 2px solid #e9ecef; padding-bottom: 10px; }}
        .source-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin-top: 20px; }}
        .source-item {{ display: flex; flex-direction: column; }}
        .source-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }}
        textarea {{ width: 100%; height: 150px; padding: 10px; border: 1px solid #dee2e6; border-radius: 6px; font-family: monospace; font-size: 12px; }}
        .button-container {{ text-align: center; margin-top: 30px; }}
        #copyButton {{ font-size: 1.2rem; padding: 15px 30px; cursor: pointer; background: #28a745; color: white; border: none; border-radius: 8px; }}
        #copyButton:hover {{ background: #218838; }}
        #copyStatus {{ margin-top: 15px; font-weight: bold; color: #28a745; visibility: hidden; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🐎 Integrated Data Collector</h1>
        <p>Paste the source code for each site into the corresponding text box below. When finished, click the button at the bottom to copy all data to your clipboard.</p>
        {sections_html}
        <div class="button-container">
            <button id="copyButton">Copy All Data to Clipboard</button>
            <p id="copyStatus">✅ All data copied to clipboard! You can now run the parser.</p>
        </div>
    </div>
    <script>
        document.getElementById('copyButton').addEventListener('click', () => {{
            const textareas = document.querySelectorAll('textarea');
            let masterString = '';
            textareas.forEach(ta => {{
                if (ta.value.trim() !== '') {{
                    const source_id = ta.id.replace(/_/g, ' '); // Recreate name from ID
                    masterString += `---START ${{source_id}}---\\n`;
                    masterString += ta.value.trim();
                    masterString += `\\n---END ${{source_id}}---\\n\\n`;
                }}
            }});
            navigator.clipboard.writeText(masterString).then(() => {{
                document.getElementById('copyStatus').style.visibility = 'visible';
                setTimeout(() => {{
                    document.getElementById('copyStatus').style.visibility = 'hidden';
                }}, 3000);
            }}, (err) => {{
                console.error('Could not copy text: ', err);
                alert('Failed to copy data. Please check browser permissions.');
            }});
        }});
    </script>
</body>
</html>
    """
    
    try:
        with open(helper_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"✅ Successfully generated dashboard at: {helper_path.resolve()}", file=sys.stdout)
        webbrowser.open(f"file://{helper_path.resolve()}")
    except Exception as e:
        print(f"❌ Could not create or open the Integrated Collector file: {e}", file=sys.stderr)

if __name__ == "__main__":
    config = load_config()
    if config:
        create_and_launch_link_helper(config)
    else:
        print("Failed to load config.json. Exiting.", file=sys.stderr)
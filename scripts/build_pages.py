#!/usr/bin/env python3
import html
import os
import subprocess

# Configuration
GOOGLE_VERIFICATION = "HYRZucheY6Ld7PfgD_K8gLlo1o3-TXZ_uMSOSFptwXE"
OUTPUT_DIR = "public"
README_FILE = "README.md"
CSS_FILE = f"{OUTPUT_DIR}/custom.css"
HTML_FILE = f"{OUTPUT_DIR}/index.html"

CUSTOM_CSS = """\
html { 
    color: #c9d1d9; 
    background-color: #0d1117; 
    color-scheme: dark; 
}
body { 
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji"; 
    font-size: 16px;
    line-height: 1.6;
    max-width: 850px; 
    margin: 0 auto; 
    padding: 40px; 
    word-wrap: break-word;
}
h1, h2, h3, h4, h5, h6 {
    margin-top: 24px;
    margin-bottom: 16px;
    font-weight: 600;
    line-height: 1.25;
    color: #e6edf3;
}
h1 { font-size: 2em; border-bottom: 1px solid #21262d; padding-bottom: 0.3em; }
h2 { font-size: 1.5em; border-bottom: 1px solid #21262d; padding-bottom: 0.3em; }
h3 { font-size: 1.25em; }
a { 
    color: #58a6ff; 
    text-decoration: none; 
}
a:hover { 
    text-decoration: underline; 
}
img { 
    max-width: 100%; 
    height: auto; 
    display: block; 
    margin: 2em auto; 
    border-radius: 6px;
    border: 1px solid #30363d;
}
pre, code {
    font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace;
}
pre { 
    background-color: #161b22; 
    padding: 16px; 
    border-radius: 6px; 
    border: 1px solid #30363d; 
    overflow: auto; 
}
code {
    background-color: rgba(110, 118, 129, 0.4);
    padding: 0.2em 0.4em;
    border-radius: 6px;
    font-size: 85%;
}
pre > code {
    background-color: transparent;
    padding: 0;
    font-size: 100%;
}
blockquote {
    padding: 0 1em;
    color: #8b949e;
    border-left: 0.25em solid #30363d;
    margin: 0;
}
"""

MERMAID_SCRIPT = '<script type="module">import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs"; mermaid.initialize({ startOnLoad: true, theme: "dark" });</script>'
GOOGLE_META = f'<meta name="google-site-verification" content="{GOOGLE_VERIFICATION}" />'


def main():
    # Step 1: Ensure directory structure and write CSS
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(CSS_FILE, "w") as f:
        f.write(CUSTOM_CSS)
    print(f"✓ Created {CSS_FILE}")

    # Step 2: Compile Markdown to Standalone HTML via Pandoc
    print("Running pandoc compilation...")
    try:
        # Pass 'custom.css' as the relative reference the HTML will use internally
        subprocess.run(
            [
                "pandoc",
                README_FILE,
                "-s",
                "-c",
                "custom.css",
                "-V",
                "pagetitle=BlinkView — Embedded Cross-Device Debugging Tool",
                "-o",
                HTML_FILE,
            ],
            check=True,
        )
        print(f"✓ Compiled {HTML_FILE} via Pandoc")
    except subprocess.CalledProcessError as e:
        print(f"❌ Pandoc compilation failed: {e}")
        return

    # Step 3: Parse and modify the output HTML
    with open(HTML_FILE, "r+") as f:
        content = f.read()

        # --- Fix Mermaid code block markup and HTML entities ---
        bad_start = '<pre class="mermaid"><code>'
        bad_end = "</code></pre>"

        if bad_start in content:
            parts = content.split(bad_start)
            for i in range(1, len(parts)):
                subparts = parts[i].split(bad_end, 1)
                cleaned_text = html.unescape(subparts[0])
                # Safely reconstruct without the inner <code> nesting
                parts[i] = subparts[1]
                parts[i - 1] = parts[i - 1] + '<pre class="mermaid">' + cleaned_text + "</pre>"
            content = "".join(parts)
            print("✓ Fixed Mermaid HTML entities and container blocks")

        # --- Inject Google Verification into <head> ---
        if "</head>" in content:
            content = content.replace("</head>", f"{GOOGLE_META}\n</head>", 1)
            print("✓ Injected Google Verification Meta Tag")

        # --- Inject Mermaid JavaScript Engine into <body> ---
        if "</body>" in content:
            content = content.replace("</body>", f"{MERMAID_SCRIPT}\n</body>", 1)
            print("✓ Injected Mermaid JS Component")

        f.seek(0)
        f.write(content)
        f.truncate()

    print("\n🎉 Documentation build fully completed!")


if __name__ == "__main__":
    main()

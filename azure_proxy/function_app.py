"""
Azure Function HTTP relay proxy.
Deployed across multiple Azure regions to provide diverse outbound IPs.
Secured via function-level auth key.
"""
import azure.functions as func
import json
import logging
import requests

app = func.FunctionApp()


@app.route(route="fetch", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def fetch_page(req: func.HttpRequest) -> func.HttpResponse:
    """
    Relay an HTTP GET request from the Azure Function's IP.
    
    Expected JSON body:
    {
        "url": "https://www.njuskalo.hr/...",
        "headers": { ... },
        "cookies": { ... },
        "timeout": 15
    }
    """
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON body"}),
            status_code=400,
            mimetype="application/json"
        )

    url = body.get("url")
    if not url:
        return func.HttpResponse(
            json.dumps({"error": "Missing 'url' field"}),
            status_code=400,
            mimetype="application/json"
        )

    # Validate URL scheme
    if not url.startswith(("https://www.njuskalo.hr", "https://njuskalo.hr")):
        return func.HttpResponse(
            json.dumps({"error": "URL must be on njuskalo.hr domain"}),
            status_code=403,
            mimetype="application/json"
        )

    headers = body.get("headers", {})
    cookies = body.get("cookies", {})
    timeout = body.get("timeout", 15)

    try:
        resp = requests.get(
            url,
            headers=headers,
            cookies=cookies,
            timeout=timeout,
            allow_redirects=True
        )

        return func.HttpResponse(
            json.dumps({
                "status_code": resp.status_code,
                "body": resp.text,
                "headers": dict(resp.headers),
                "url": resp.url
            }),
            status_code=200,
            mimetype="application/json"
        )

    except requests.Timeout:
        logging.warning(f"Timeout fetching {url}")
        return func.HttpResponse(
            json.dumps({"error": "Upstream request timed out"}),
            status_code=504,
            mimetype="application/json"
        )
    except requests.RequestException as e:
        logging.error(f"Request error for {url}: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Request failed: {str(e)}"}),
            status_code=502,
            mimetype="application/json"
        )

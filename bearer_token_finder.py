
import asyncio
from playwright.async_api import async_playwright

AD_URL = "https://www.njuskalo.hr/nekretnine/iznajmljujem-2s-stan-65m2-siget-avenue-mall-2-cimerice-oglas-37026812"

async def get_bearer_token_and_cookies(ad_url=AD_URL, headless=True):
    async with async_playwright() as p:
        # Headless evasion: disable AutomationControlled, set UA, viewport, and navigator.webdriver
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ]
        browser = await p.chromium.launch(headless=headless, args=launch_args)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        page = await context.new_page()
        # Patch navigator.webdriver to false
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        await page.goto(ad_url)
        # Accept consent dialog if present
        try:
            await page.wait_for_selector('#didomi-notice-agree-button', timeout=5000)
            await page.click('#didomi-notice-agree-button')
        except Exception:
            pass
        # Intercept the phone API request
        phone_api_url_part = "/ccapi/v4/phone-numbers/ad/"
        token = None
        api_request_seen = False
        def handle_request(request):
            nonlocal token, api_request_seen
            if phone_api_url_part in request.url:
                headers = request.headers
                # Extract Bearer token from Authorization header
                auth = headers.get("authorization")
                if auth and auth.lower().startswith("bearer "):
                    token = auth[7:]
                api_request_seen = True
        page.on("request", handle_request)
        # Wait for the correct phone button inside the contact entry with phone icon
        phone_button_selector = (
            'li.ClassifiedDetailOwnerDetails-contactEntry:has(i.icon--classifiedDetailViewPhone) '
            'button.UserPhoneNumber-callSeller'
        )
        print("[DEBUG] Waiting for phone button with selector:", phone_button_selector)
        await page.wait_for_selector(phone_button_selector, timeout=10000)
        await page.click(phone_button_selector)
        # Wait for the phone API request to be captured
        for _ in range(20):
            if token and api_request_seen:
                break
            await asyncio.sleep(0.5)
        # Get all cookies from the browser context after the API call
        cookies = await context.cookies()
        cookie_dict = {c.get('name'): c.get('value') for c in cookies if c.get('name') and c.get('value')}
        await browser.close()
        return token, cookie_dict

if __name__ == "__main__":
    async def _main():
        # Always use headless=False for manual runs (headful mode)
        token, cookies = await get_bearer_token_and_cookies(headless=True)
        print("\nCookies (Python dict):\n")
        print(cookies)
        if token:
            print("\nBearer token found in localStorage:")
            print(token)
        else:
            print("\nBearer token not found in localStorage. You may need to inspect network requests for Authorization headers.")
    asyncio.run(_main())
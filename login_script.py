from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import time
import sys

def log_message(message):
    """Helper function to log messages with timestamp"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{timestamp}] {message}")

def keep_alive():
    # YOUR APP URL - CHANGE THIS!
    APP_URL = "https://football-predictions-agility.streamlit.app/"  # ← PUT YOUR URL HERE!
    
    with sync_playwright() as p:
        try:
            # Launch browser in headless mode for CI/CD
            log_message("Launching browser in headless mode...")
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage'],
                slow_mo=100
            )
            context = browser.new_context(viewport={'width': 1280, 'height': 800})
            page = context.new_page()
            
            # Navigate to your app (NO LOGIN NEEDED!)
            log_message(f"Navigating to {APP_URL}...")
            try:
                page.goto(APP_URL, timeout=60000)
                log_message("Page loaded successfully")
            except PlaywrightTimeoutError:
                log_message("Timeout while loading the page")
                raise
            
            # Wait for the page to fully load
            log_message("Waiting for page to fully load...")
            page.wait_for_load_state("networkidle", timeout=30000)
            log_message("Page is fully loaded!")
            
            # Optional: Take a screenshot to verify it worked
            try:
                page.screenshot(path="app_screenshot.png")
                log_message("Screenshot saved as app_screenshot.png")
            except Exception as e:
                log_message(f"Could not save screenshot: {str(e)}")
            
            # Wait for 10 minutes to keep the app alive
            wait_minutes = 10
            log_message(f"App is active! Waiting for {wait_minutes} minutes...")
            for remaining in range(wait_minutes * 60, 0, -60):
                log_message(f"Time remaining: {remaining//60} minutes")
                time.sleep(60)  # Sleep for 1 minute at a time
            
            log_message(f"{wait_minutes} minutes have passed! Mission accomplished!")
            
        except Exception as e:
            log_message(f"An error occurred: {str(e)}")
            log_message(f"Error type: {type(e).__name__}")
            import traceback
            log_message("Traceback:\n" + traceback.format_exc())
            sys.exit(1)
            
        finally:
            # Close the browser
            try:
                log_message("Closing browser...")
                browser.close()
                log_message("Browser closed successfully")
            except Exception as e:
                log_message(f"Error while closing browser: {str(e)}")

if __name__ == "__main__":
    log_message("🚀 Keep-Alive Script Started (No Login Version)")
    try:
        keep_alive()
    except Exception as e:
        log_message(f"Script failed: {str(e)}")
        sys.exit(1)
    log_message("✅ Script completed successfully")

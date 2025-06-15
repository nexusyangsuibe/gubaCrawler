from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException,ElementClickInterceptedException
from PIL import Image,ImageChops,ImageDraw
from datetime import datetime
from collections import deque
from typing import Optional,List
import multiprocessing as mp
import pandas as pd
import numpy as np
import traceback
import pickle
import json
import time
import os
import re

import logging
logging.basicConfig(level=logging.INFO)

# common tool functions are as follows
def outputAsXlsx(
    df: pd.DataFrame,
    output_filename: str,
    output_pathname: str,
    threshold_rows: int = 1_000_000,
    threshold_GB: float = 4.0
) -> None:
    """
    Export a DataFrame to one or more xlsx files, splitting if too large by row count or memory size.

    Args:
        df: The DataFrame to output.
        output_filename: The desired base filename (e.g., "result.xlsx").
        output_pathname: Directory path to output files.
        threshold_rows: Maximum number of rows per file.
        threshold_GB: Maximum size (in GB) per file.

    Returns:
        None
    """
    logger = logging.getLogger("outputAsXlsx")

    def calc_memory_usage_gb(sub_df: pd.DataFrame) -> float:
        """Calculate the memory usage of a DataFrame in GB."""
        return sub_df.memory_usage(deep=True).sum() / (1024 ** 3)

    def find_best_bulk_num(df: pd.DataFrame, threshold_GB: float) -> int:
        """
        Find the optimal number of splits (bulk_num) so that each chunk's memory usage is below threshold_GB.

        Returns:
            Number of chunks to split into.
        """
        bulk_num = 1
        while True:
            fits = True
            for idx in range(bulk_num):
                # Calculate the row range for this chunk
                start = int(len(df) * (idx / bulk_num))
                end = int(len(df) * ((idx + 1) / bulk_num))
                chunk = df.iloc[start:end]
                # If any chunk exceeds the threshold, increase bulk_num
                if calc_memory_usage_gb(chunk) > threshold_GB:
                    fits = False
                    break
            if fits:
                return bulk_num
            bulk_num += 1

    def output_bulk(df_bulk: pd.DataFrame, fileName: str, threshold_GB: float) -> None:
        """
        Output a DataFrame (or chunk) to one or more xlsx files, splitting further by memory if needed.
        """
        bulk_num = find_best_bulk_num(df_bulk, threshold_GB)
        if bulk_num == 1:
            # Data fits in a single file
            df_bulk.to_excel(fileName)
            logger.info(f"Written {fileName}.")
        else:
            # Data needs to be split into several files due to memory size
            logger.info(f"File {fileName} exceeds {threshold_GB}GB, splitting into {bulk_num} parts.")
            file_rows = df_bulk.shape[0]
            for i in range(bulk_num):
                fileName_ = f"{os.path.splitext(fileName)[0]}_{i+1}.xlsx"
                logger.info(f"Writing chunk: {fileName_}")
                # Calculate row range for this chunk
                start = int(file_rows * (i / bulk_num))
                end = int(file_rows * ((i + 1) / bulk_num))
                df_bulk.iloc[start:end].to_excel(fileName_)

    # Ensure output_pathname ends with an appropriate separator (or is empty)
    output_path = output_pathname if output_pathname.endswith(os.sep) or output_pathname == "" else output_pathname + os.sep
    file_num = df.shape[0] // threshold_rows  # How many full-sized files will be needed

    logger.info(f"Total rows: {df.shape[0]}, dividing into {file_num + 1} files.")

    if file_num == 0:
        # Data fits in one file by row count; check memory constraint
        fileName = f"{output_path}{os.path.splitext(output_filename)[0]}.xlsx"
        output_bulk(df, fileName, threshold_GB)
    else:
        # Data must be split by row count
        file_rows, last_rows = divmod(df.shape[0], file_num + 1)
        # Add remainder to the last file
        last_rows = file_rows + last_rows
        logger.info(f"First {file_num} files: {file_rows} rows each, last file: {last_rows} rows.")
        for idx in range(file_num):
            # Output each chunk
            df_bulk = df.iloc[idx * file_rows : (idx + 1) * file_rows]
            fileName = f"{output_path}{os.path.splitext(output_filename)[0]}_{idx+1}.xlsx"
            output_bulk(df_bulk, fileName, threshold_GB)
        if last_rows:
            # Output the last chunk (which may be larger due to the remainder)
            df_bulk = df.iloc[file_num * file_rows :]
            fileName = f"{output_path}{os.path.splitext(output_filename)[0]}_{file_num+1}.xlsx"
            output_bulk(df_bulk, fileName, threshold_GB)


def create_webdriver(
    headless: bool = True,
    stealth_js_path: str = "stealth.min.js",
    driver_path: str = None,
    max_retries: int = 3,
    retry_wait: int = 2
) -> webdriver.Edge:
    """
    Create and configure a Selenium Edge WebDriver instance with anti-detection and XHR/fetch interception.

    Args:
        headless: Whether to run Edge in headless mode (no UI).
        stealth_js_path: Path to the stealth.min.js script for anti-bot evasion.
        driver_path: Optional path to msedgedriver executable.
        max_retries: How many times to retry upon failure.
        retry_wait: Seconds to wait between retries.

    Returns:
        Configured Edge WebDriver instance.

    Raises:
        WebDriverException: If unable to create the driver after retries.
    """
    logger = logging.getLogger("create_webdriver")

    options = Options()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--log-level=3")
    options.add_argument("--inprivate")
    if headless:
        options.add_argument("--headless")

    # XHR/fetch interception script (for grabbing replies)
    xhr_intercept_script = r"""
    (function() {
    window._interceptedResponses = [];
    (function(originalOpen, originalSend) {
        XMLHttpRequest.prototype.open = function(method, url) {
        this._url = url;
        return originalOpen.apply(this, arguments);
        };
        XMLHttpRequest.prototype.send = function(body) {
        var self = this;
        var originalOnReadyStateChange = self.onreadystatechange;
        self.onreadystatechange = function() {
            if (self.readyState === 4 && self.status === 200) {
            if (self._url && self._url.includes("reply")) { // use the proper filter condition here
                window._interceptedResponses.push({
                type: 'XHR',
                url: self._url,
                status: self.status,
                response: self.responseText
                });
            }
            }
            if (originalOnReadyStateChange) {
            return originalOnReadyStateChange.apply(self, arguments);
            }
        };
        return originalSend.apply(this, arguments);
        };
    })(XMLHttpRequest.prototype.open, XMLHttpRequest.prototype.send);
    if (window.fetch) {
        var originalFetch = window.fetch;
        window.fetch = function() {
        return originalFetch.apply(this, arguments)
            .then(function(response) {
            var responseClone = response.clone();
            responseClone.text().then(function(bodyText) {
                if (response.url.includes("reply")) {
                window._interceptedResponses.push({
                    type: 'fetch',
                    url: response.url,
                    status: response.status,
                    response: bodyText
                });
                }
            });
            return response;
            });
        };
    }
    })();
    """

    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            # Create Edge driver; optionally with a custom path
            driver = webdriver.Edge(options=options, executable_path=driver_path) if driver_path else webdriver.Edge(options=options)

            # Inject stealth script for anti-bot detection, if available
            try:
                with open(stealth_js_path, "r", encoding="utf-8") as f:
                    stealth_code = f.read()
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": stealth_code})
                logger.info("Injected stealth.min.js for anti-bot evasion.")
            except Exception as e:
                logger.info(f"Could not inject stealth.min.js: {e}")

            # Inject reply interception script
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": xhr_intercept_script})
            logger.info("Injected XHR/fetch interception script.")

            logger.info("WebDriver created successfully.")
            return driver
        except Exception as e:
            last_exception = e
            logger.info(f"Attempt {attempt} to create WebDriver failed: {e}")
            logger.debug(traceback.format_exc())
            time.sleep(retry_wait)

    logger.info(f"Failed to create Edge WebDriver after {max_retries} attempts.")
    raise WebDriverException("Could not create Edge WebDriver.") from last_exception


def human_verify_until_success(driver: WebDriver, stock_code: str) -> bool:
    """
    Attempt to bypass human verification on the current page until successful.

    This function checks for common signs of a human verification challenge (such as a special title
    or a visible verification iframe) and repeatedly tries to solve it using the `human_verification`
    function. If verification fails, it refreshes the page and tries again.

    Args:
        driver: The Selenium WebDriver instance.
        stock_code: String stock code for logging/context.
        retry_wait: Seconds to wait between retries.

    Returns:
        True if verification is successful.
    """
    logger = logging.getLogger("human_verify_until_success")

    while True:
        # Check for verification challenge: title or visible iframe
        title_is_verification = driver.title == "身份核实"
        iframes = driver.find_elements(by=By.CLASS_NAME, value="popwscps_d_iframe")
        iframe_displayed = bool(iframes and iframes[0].is_displayed())

        if title_is_verification or iframe_displayed:
            # Try to solve the challenge
            driver.execute_script("typeof tk_tg_zoomin === 'function' && tk_tg_zoomin()")
            verified = human_verification(driver, stock_code)
            if verified == "success":
                logger.info("Human verification successful.")
                return True
            elif verified == "fail":
                logger.info("Human verification failed. Refreshing page and retrying.")
                driver.refresh()
            else:  # "double check" or unexpected return
                logger.info("Waiting briefly before re-checking verification status.")
        else:
            # No verification challenge found; assume success
            logger.info("No human verification present. Continuing execution.")
            return True


def get_guba_table(
    stock_code: str,
    start_year: int,
    current_page: int,
    max_retries_per_page: int = 3,
    retry_wait: int = 2,
    max_continuous_fail_pages: int = 3
) -> Optional[bool]:
    """
    Crawl the main table/listing of posts for a given stock code from Eastmoney Guba.

    Args:
        stock_code: The target stock code as a string.
        start_year: Min year (inclusive) for posts to crawl, by post's publish date.
        current_page: The starting page number (1-based).
        max_retries_per_page: Maximum retry attempts per web page.
        retry_wait: Seconds to wait between retries.
        max_continuous_fail_pages: Max number of consecutive failed pages before aborting.

    Returns:
        True if completed successfully, or None if failed on all attempts for every page, or raises if too many continuous failures.
    """
    logger = logging.getLogger("get_guba_table")
    driver = create_webdriver()
    continuous_fail_count = 0
    try:
        url = f"https://guba.eastmoney.com/list,{stock_code}_{current_page}.html"
        driver.get(url)
        time.sleep(1)
        driver.execute_script("typeof tk_tg_zoomin === 'function' && tk_tg_zoomin()")

        # Initial CAPTCHA check
        if driver.title == "身份核实" or (
            (iframes := driver.find_elements(by=By.CLASS_NAME, value="popwscps_d_iframe"))
            and iframes[0].is_displayed()
        ):
            logger.info(f"{driver.current_url} triggered CAPTCHA for {stock_code}")
            human_verify_until_success(driver, stock_code)

        # Stock redirect check
        title_code_match = re.search(r"\d{6}", driver.title)
        if title_code_match and title_code_match.group() != stock_code:
            logger.info(f"{url} auto-redirected to another stock. Pausing for 1 hour and retrying.")
            driver.quit()
            time.sleep(3600)
            # Recursively restart for this stock and page
            return get_guba_table(stock_code, start_year, current_page, max_retries_per_page, retry_wait, max_continuous_fail_pages)

        # Get max page number
        max_page_num = int(driver.find_elements(by=By.CLASS_NAME, value="nump")[-1].text)
        while current_page <= max_page_num:
            page_success = False
            for page_attempt in range(1, max_retries_per_page + 1):
                try:
                    # CAPTCHA check before crawling
                    if driver.title == "身份核实" or (
                        (iframes := driver.find_elements(by=By.CLASS_NAME, value="popwscps_d_iframe"))
                        and iframes[0].is_displayed()
                    ):
                        logger.info(f"{driver.current_url} triggered CAPTCHA for {stock_code}")
                        human_verify_until_success(driver, stock_code)

                    # Extract article list (JS variable on the page)
                    article_list = driver.execute_script("return article_list")
                    df = pd.DataFrame(article_list["re"])[[
                        "media_type", "post_click_count", "post_comment_count", "post_forward_count",
                        "post_from_num", "post_has_pic", "post_id", "post_display_time",
                        "post_last_time", "post_publish_time"
                    ]]
                    # Extract article URLs
                    content_navi = [
                        node.find_element(by=By.TAG_NAME, value="a").get_property("href")
                        for node in driver.find_element(by=By.CLASS_NAME, value="listbody")
                        .find_elements(by=By.CLASS_NAME, value="title")
                    ]
                    df["link_url"] = content_navi

                    # Save this page's data
                    start_date = datetime.strptime(min(df["post_publish_time"]), "%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
                    end_date = datetime.strptime(max(df["post_publish_time"]), "%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
                    out_dir = f"respawnpoint/{stock_code}"
                    os.makedirs(out_dir, exist_ok=True)
                    out_file = f"{out_dir}/{stock_code}_{current_page}_{start_date}_{end_date}.pkl"
                    with open(out_file, "wb") as f:
                        pickle.dump(df, f)
                    logger.info(f"Saved page {current_page} for stock {stock_code}: {out_file}")
                    page_success = True
                    continuous_fail_count = 0  # Reset fail count on success
                    break  # Page succeeded, move to next page

                except Exception as e:
                    logger.info(f"Error crawling page {current_page} (attempt {page_attempt}/{max_retries_per_page}) for {stock_code}", exc_info=True)
                    time.sleep(retry_wait)
                    driver.refresh()
            if not page_success:
                continuous_fail_count += 1
                logger.info(f"Failed to crawl page {current_page} for {stock_code} after {max_retries_per_page} attempts, skipping this page. Continuous fail count: {continuous_fail_count}")
                if continuous_fail_count >= max_continuous_fail_pages:
                    logger.info(f"Aborting: {continuous_fail_count} consecutive page failures for stock {stock_code}.")
                    driver.quit()
                    raise RuntimeError(f"Aborted after {continuous_fail_count} consecutive failed pages for stock {stock_code}")

            # Move to next page if end_date is not before start_year
            try:
                end_date_year = int(end_date[:4])
            except Exception:
                logger.info(f"Could not parse end_date for page {current_page} of {stock_code}, assuming end.")
                break
            if current_page < max_page_num and end_date_year >= start_year:
                current_page += 1
                # Go to next page
                try:
                    next_page_button = driver.find_element(by=By.CLASS_NAME, value="nextp")
                    actions = ActionChains(driver)
                    actions.move_to_element(next_page_button).perform()
                    try:
                        next_page_button.click()
                    except ElementClickInterceptedException as e:
                        covering_element = re.search(r'Other element would receive the click: (<.+>)', e.msg).group(1)
                        tag_name = re.match(r'<\s*(\w+)', covering_element).group(1)
                        attributes = re.findall(r'(\w+)\s*=\s*"([^"]+)"', covering_element)
                        filtered_attrs = [(name, value) for name, value in attributes if name.lower() != "style"]
                        css_selector = tag_name + "".join([f'[{attr_name}="{attr_value}"]' for attr_name, attr_value in filtered_attrs])
                        driver.execute_script(f"document.querySelector('{css_selector}').style.display='none'")
                        next_page_button = driver.find_element(by=By.CLASS_NAME, value="nextp")
                        next_page_button.click()
                except Exception as e:
                    logger.info(f"Failed to go to next page from {current_page} for {stock_code}: {e}")
                    break
            else:
                # Clean up screenshots for this stock
                screen_shots = [file for file in os.listdir("recapcha") if file.startswith(f"{stock_code}_screenshot")]
                for screen_shot in screen_shots:
                    os.remove(os.path.join("recapcha", screen_shot))
                logger.info(f"Completed crawling for stock {stock_code}.")
                break
        driver.quit()
        return True
    except Exception as e:
        logger.info(f"Critical failure for stock {stock_code}: {e}", exc_info=True)
        driver.quit()
        return None


def get_guba_content(
    stock_code: str,
    continuous_fail_count: int = 0,
    max_retries_per_post: int = 3,
    max_continuous_fail_posts: int = 3,
    retry_wait: int = 2,
) -> Optional[bool]:
    """
    Fetch and append detailed content for each post of a given stock.

    Args:
        stock_code (str): The stock code.
        continuous_fail_count (int): Counter for consecutive failures, used to control retry or stop logic.
        max_retries_per_post (int): Maximum retry attempts per post URL.
        max_continuous_fail_posts (int): Maximum number of consecutive failed posts before aborting.
        retry_wait (int): Number of seconds to wait between retries.
        
    Returns:
        True if completed successfully, or None if a critical failure occurs.
    """
    logger = logging.getLogger("get_guba_content")
    continuous_fail_count = 0  # reset at beginning
    url = None
    try:
        # Find the file and load the DataFrame
        filename = [file for file in os.listdir("respawnpoint/afinished") if file.startswith(stock_code)][0]
        df = pickle.load(open(f"respawnpoint/afinished/{filename}", "rb"))
        cols = df.columns

        # Initialize the necessary columns if they don't exist
        if not all(col in cols for col in ["content", "ip", "post_user", "reply"]):
            df["content"] = None
            df["ip"] = None
            df["post_user"] = None
            df["reply"] = None
            start_idx = 0
        else:
            # Continue from the next unsaved post (if partially saved)
            start_idx = df[["content", "ip", "post_user", "reply"]].last_valid_index() + 1

        len_df = df.shape[0]
        link_urls = df["link_url"]
        max_interval = 240

        driver = create_webdriver()
        current_idx = start_idx

        while current_idx < len_df:
            # Determine the segment boundaries; use segments to prevent a too-long continuous run
            to_rows = min(current_idx + max_interval, len_df)
            segment_range = range(current_idx, to_rows)

            # Initialize segment-specific storage
            contents = [None] * (to_rows - current_idx)
            ips = [None] * (to_rows - current_idx)
            post_users = [None] * (to_rows - current_idx)
            replies = [None] * (to_rows - current_idx)

            idx = current_idx

            while idx < to_rows:
                seg_offset = idx - current_idx
                url = link_urls.loc[idx]
                post_success = False
                content = ip = post_user = reply = None

                for attempt in range(1, max_retries_per_post + 1):
                    try:
                        # Fetch the URL and execute initial script
                        driver.get(url)
                        time.sleep(1)
                        driver.execute_script("typeof tk_tg_zoomin === 'function' && tk_tg_zoomin()")
                        
                        # Check for CAPTCHA and resolve if necessary
                        if driver.title == "身份核实" or (
                            (iframes := driver.find_elements(by=By.CLASS_NAME, value="popwscps_d_iframe"))
                            and iframes[0].is_displayed()
                        ):
                            logger.info(f"{driver.current_url} triggered CAPTCHA for {stock_code}")
                            human_verify_until_success(driver, stock_code)
                        
                        # Continue if the page returns a 404-like response.
                        # For pages on eastmoney, check both <pre> text and JS variables.
                        if driver.find_elements(by=By.TAG_NAME, value="pre") and \
                           driver.find_element(by=By.TAG_NAME, value="pre").text == "Not Found":
                            logger.info(f"{url} returns 404 Not Found")
                            raise ValueError("404 Not Found")
                        
                        if "guba.eastmoney.com" in url:
                            post_article = driver.execute_script(
                                'return (typeof post_article !== "undefined" && post_article) ? post_article : undefined'
                            )
                            if not post_article:
                                logger.info(f"{url} returns 404 Not Found")
                                raise ValueError("404 Not Found")
                            else:
                                content = post_article.get("post_content")
                                ip = post_article.get("post_ip_address") or None
                                post_user = post_article.get("post_user")
                                intercepted = driver.execute_script("return window._interceptedResponses;")
                                for entry in intercepted:
                                    if "reply/api/Reply/ArticleNewReplyList" in entry["url"]:
                                        r = json.loads(entry['response'])
                                        reply = r["re"] if isinstance(r["re"], list) else []
                                        break
                        else:
                            # Assume the post is from caifuhao or a similar URL
                            content = driver.execute_script("return articleTxt")
                            ip_elements = driver.find_elements(
                                by=By.XPATH, value='//*[@id="main"]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/span[3]'
                            )
                            ip = ip_elements[0].text.strip() if ip_elements else None
                            reply = driver.execute_script(
                                'return (typeof replyListAll !== "undefined" && replyListAll) ? replyListAll : (typeof reply_list !== "undefined") ? reply_list : undefined'
                            )
                            if reply:
                                reply = reply.get("re")
                        
                        # If reached here, the post has been successfully processed.
                        continuous_fail_count = 0
                        post_success = True
                        break  # exit retry loop for this post

                    except Exception as e:
                        logger.info(
                            f"Error fetching URL {url} (attempt {attempt}/{max_retries_per_post}) for {stock_code}",
                            exc_info=True,
                        )
                        time.sleep(retry_wait)
                        driver.refresh()
                
                # If after all retry attempts the post is not successfully crawled:
                if not post_success:
                    continuous_fail_count += 1
                    logger.info(
                        f"Failed to fetch URL {url} for {stock_code} after {max_retries_per_post} attempts. "
                        f"Continuous fail count: {continuous_fail_count}"
                    )
                    if continuous_fail_count >= max_continuous_fail_posts:
                        logger.info(
                            f"Aborting: {continuous_fail_count} consecutive post failures for stock {stock_code}."
                        )
                        driver.quit()
                        raise RuntimeError(
                            f"Aborted after {continuous_fail_count} consecutive failed posts for stock {stock_code}"
                        )
                else:
                    # Successful retrieval: save the fetched details in the segment arrays.
                    contents[seg_offset] = content
                    ips[seg_offset] = ip
                    post_users[seg_offset] = post_user
                    replies[seg_offset] = reply

                idx += 1

            # Update the DataFrame with the newly collected data for the segment.
            df.loc[segment_range, "content"] = contents
            df.loc[segment_range, "ip"] = ips
            df.loc[segment_range, "post_user"] = pd.Series(post_users, index=segment_range, dtype="object")
            df.loc[segment_range, "reply"] = pd.Series(replies, index=segment_range, dtype="object")
            pickle.dump(df, open(f"respawnpoint/afinished/{filename}", "wb"))
            logger.info(f"{stock_code} has finished segment {to_rows}/{len_df}")
            
            current_idx = to_rows

        # Once fully completed, store the finished file.
        pickle.dump(df, open(f"respawnpoint/finished/{filename}", "wb"))
        driver.quit()
        return True

    except Exception as e:
        logger.info(f"Critical failure for stock {stock_code}: {e}", exc_info=True)
        driver.quit()
        return None



def human_verification(
    driver: WebDriver,
    stock_code: str,
    wait_time: int = 2,
) -> str:
    """
    Attempt to bypass a slider CAPTCHA on Eastmoney Guba.

    Args:
        driver: Selenium WebDriver instance.
        stock_code: The stock code, for logging/context.
        wait_time: Time (seconds) to wait after the function.

    Returns:
        "success" if verification passed and cleared,
        "double check" if verification solved but a new one appears immediately,
        "fail" if cannot locate/solve the slider or unknown error.
    """
    def flood_fill(img, start, visited):
        """Perform flood fill and return all connected white pixel coordinates."""
        width, height = img.size
        pixels = img.load()
        x0, y0 = start
        component = []
        queue = deque([(x0, y0)])
        visited[y0][x0] = True
        while queue:
            x, y = queue.popleft()
            component.append((x, y))
            # Check 4-connected neighbors (add diagonals if desired)
            for nx, ny in ((x-1, y), (x+1, y), (x, y-1), (x, y+1)):
                if 0 <= nx < width and 0 <= ny < height:
                    if not visited[ny][nx] and pixels[nx, ny] != 0:
                        visited[ny][nx] = True
                        queue.append((nx, ny))
        return component
    def get_all_components(img):
        """Find and return all connected components (each puzzle piece) in the binary image."""
        width, height = img.size
        visited = [[False] * width for _ in range(height)]
        components = []
        pixels = img.load()
        for y in range(height):
            for x in range(width):
                if not visited[y][x] and pixels[x, y] != 0:
                    comp = flood_fill(img, (x, y), visited)
                    components.append(comp)
        return components
    def get_boundaries(comp):
        xs, ys = zip(*comp)
        bounding_box = (min(xs), min(ys), max(xs) - min(xs) + 1, max(ys) - min(ys) + 1)
        return bounding_box
    def bg_img_loaded(driver,bg_img_url):
        is_loaded=driver.execute_script("var img = new Image(); img.src = arguments[0]; return img.complete;", bg_img_url)
        return is_loaded
    logger = logging.getLogger("human_verification")
    iframe=None
    iframes=driver.find_elements(by=By.CLASS_NAME,value="popwscps_d_iframe")
    if driver.title!="身份核实" and not (iframes and iframes[0].is_displayed()) and not ((divCaptcha:=driver.find_elements(by=By.ID,value="divCaptcha")) and divCaptcha[0].is_displayed()):
        return "success"
    if iframes:
        logger.info(f"{driver.current_url} triggered CAPTCHA in iframe")
        iframe=iframes[0]
        if iframe.is_displayed():
            driver.switch_to.frame(iframe)
            return human_verification(driver,stock_code)
    try:
        wait=WebDriverWait(driver,90)  # Wait up to 90 seconds, sometimes the background image is loaded slowly
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME,"em_cut_fullbg")))
        em_cut_fullbg=driver.find_element(by=By.CLASS_NAME,value="em_cut_fullbg")
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME,"em_cut_fullbg_slice")))
        em_cut_fullbg_slice0=em_cut_fullbg.find_element(by=By.CLASS_NAME,value="em_cut_fullbg_slice")
        bg_img_url=em_cut_fullbg_slice0.value_of_css_property('background-image').split('("')[1].split('")')[0] # style="background-image: url(&quot;https://smartvcode2.eastmoney.com/00/resources/e02b_160/1/f5/f5e70a42cff97a97ccbf5a15dfd6974e/f5e70a42cff97a97ccbf5a15dfd6974e.jpg&quot;);"
        wait.until(lambda driver:bg_img_loaded(driver,bg_img_url))
        em_cut_fullbg.screenshot(f"recapcha/{stock_code}_screenshot_original.png")
        em_slider=driver.find_element(by=By.CLASS_NAME,value="em_slider_knob")
        move = ActionChains(driver)
        move.click_and_hold(em_slider).perform()
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME,"em_cut_bg")))
        em_cut_bg=driver.find_element(by=By.CLASS_NAME,value="em_cut_bg")
        em_cut_bg_slice0=em_cut_bg.find_element(by=By.CLASS_NAME,value="em_cut_bg_slice")
        bg_img_url=em_cut_bg_slice0.value_of_css_property('background-image').split('("')[1].split('")')[0]
        wait.until(lambda driver:bg_img_loaded(driver,bg_img_url))
        em_cut_bg.screenshot(f"recapcha/{stock_code}_screenshot_modified.png")
        image_original=Image.open(f"recapcha/{stock_code}_screenshot_original.png")
        image_modified=Image.open(f"recapcha/{stock_code}_screenshot_modified.png")
        diff_img = ImageChops.difference(image_original,image_modified).convert("L")
        arr=np.array(diff_img)
        height,width=arr.shape
        arr[:,0]=0
        arr[0,:]=0
        for col in range(1,width):
            if np.count_nonzero(arr[:,col])/height>0.88:
                arr[:,col]=arr[:,col-1]
        for row in range(1,height):
            if np.count_nonzero(arr[row,:])/width>0.88:
                arr[row,]=arr[row-1,:]
        diff_img=Image.fromarray(arr)
        components = get_all_components(diff_img)
        pieces_info = [get_boundaries(comp) for comp in components]
        pieces_info=list(filter(lambda x:x[2]>6 and x[3]>6,pieces_info))
        draw = ImageDraw.Draw(diff_img)
        for piece in pieces_info:
            x, y, w, h = piece
            draw.rectangle([x, y, x + w, y + h], outline="red", width=1)
        diff_img.save(f"recapcha/{stock_code}_verify_recognition.png")
        pieces_info.sort()
        puzzle_piece=pieces_info[0]
        puzzle_piece_real_bottom=puzzle_piece[1]
        for idx in range(puzzle_piece[0]+1,puzzle_piece[0]+12):
            if np.count_nonzero(arr[:,idx])==np.count_nonzero(arr[:,idx-1]):
                puzzle_piece_real_bottom=np.argmax(arr[:,idx]!=0)
                break
        if len(pieces_info)==1: # overlap
            em_slice=driver.find_element(by=By.CLASS_NAME,value="em_slice")
            img_actual_width=em_slice.size["width"]*(44/60)
            overlapping=img_actual_width*2-puzzle_piece[2]
            moving_dist=img_actual_width-overlapping
        else:
            holes=pieces_info[1:]
            candidates=list(filter(lambda x:(x[1]-puzzle_piece_real_bottom)**2<1.24,holes))
            candidates.sort(key=lambda x:(x[1]-puzzle_piece[1])**2+(x[2]-puzzle_piece[2])**2+(x[3]-puzzle_piece[3])**2)
            if candidates:
                moving_dist=candidates[0][0]-puzzle_piece[0]
            else:
                if puzzle_piece[2]/puzzle_piece[3]>1.24:
                    em_slice=driver.find_element(by=By.CLASS_NAME,value="em_slice")
                    img_actual_width=em_slice.size["width"]*(44/60)
                    overlapping=img_actual_width*2-puzzle_piece[2]
                    moving_dist=img_actual_width-overlapping
                else:
                    holes.sort(key=lambda x:(x[1]-puzzle_piece[1])**2+(x[2]-puzzle_piece[2])**2+(x[3]-puzzle_piece[3])**2)
                    moving_dist=holes[0][0]-puzzle_piece[0]
        if moving_dist<=0:
            moving_dist=24
        move.move_by_offset(moving_dist+np.random.normal(0,0.24),np.random.normal(0,0.24)).release().perform()
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME,"em_info_tip")))
        em_info_tip=driver.find_element(by=By.CLASS_NAME,value="em_info_tip")
        if em_info_tip.text=="验证成功":
            driver.switch_to.default_content()
            logger.info(f"Slider CAPTCHA solved successfully for {driver.current_url}.")
            time.sleep(wait_time)
            if driver.title!="身份核实" and not (iframes and iframes[0].is_displayed()) and not ((divCaptcha:=driver.find_elements(by=By.ID,value="divCaptcha")) and divCaptcha[0].is_displayed()):
                return "success"
            else:
                logger.info(f"A second robot verification appeared (double check needed) for {driver.current_url}.")
                return "double check"
        else:
            logger.info(f"Failed to solve slider CAPTCHA for {driver.current_url}.")
            driver.switch_to.default_content()
            time.sleep(wait_time)
            return "fail"
    except:
        logger.info(f"Error occurred in solving slider CAPTCHA for {driver.current_url}.")
        driver.switch_to.default_content()
        traceback.logger.info_exc()
        time.sleep(wait_time)
        return "fail"

def generate_concated_table(
    stock_code: str,
    min_year: Optional[int] = None,
    sort_by: str = "post_publish_time",
    respawnpoint_dir: str = "respawnpoint"
) -> Optional[pd.DataFrame]:
    """
    Concatenate all crawled post tables for a stock into a single DataFrame.

    Args:
        stock_code: The stock code (string).
        min_year: If provided, filter out posts published before this year.
        sort_by: Column to sort the resulting DataFrame by (default: "post_publish_time").
        respawnpoint_dir: Directory where pickled files are stored.

    Returns:
        A pandas DataFrame with all posts, or None if no valid data is found.
    """
    logger = logging.getLogger("generate_concated_table")
    stock_dir = os.path.join(respawnpoint_dir, stock_code)
    if not os.path.isdir(stock_dir):
        logger.info(f"No data directory found for stock {stock_code}: {stock_dir}")
        return None

    tables: List[pd.DataFrame] = []
    files = sorted([f for f in os.listdir(stock_dir) if f.endswith(".pkl")])

    for file in files:
        file_path = os.path.join(stock_dir, file)
        try:
            with open(file_path, "rb") as f:
                df = pickle.load(f)
            if not isinstance(df, pd.DataFrame):
                logger.info(f"File {file_path} does not contain a DataFrame, skipping.")
                continue
            tables.append(df)
        except Exception as e:
            logger.info(f"Failed to load {file_path}: {e}")
            continue

    if not tables:
        logger.info(f"No valid post tables found for stock {stock_code}.")
        return None

    all_posts = pd.concat(tables, ignore_index=True)
    logger.info(f"Concatenated {len(tables)} tables for stock {stock_code} (total: {len(all_posts)})")

    # Optional filtering by year
    if min_year is not None and sort_by in all_posts.columns:
        try:
            all_posts[sort_by] = pd.to_datetime(all_posts[sort_by], errors='coerce')
            all_posts = all_posts[all_posts[sort_by].dt.year >= min_year]
            logger.info(f"Filtered posts before year {min_year}, remaining: {len(all_posts)}")
        except Exception as e:
            logger.info(f"Failed to filter by year for {stock_code}: {e}")

    # Optional sorting
    if sort_by in all_posts.columns:
        try:
            all_posts = all_posts.sort_values(by=sort_by, ascending=True).reset_index(drop=True)
        except Exception as e:
            logger.info(f"Failed to sort DataFrame by {sort_by}: {e}")

    return all_posts

def crwal_by_stkcd(param:Optional[List])->bool:
    """
    Multiprocessing-friendly entry for crawling, organizing, and exporting Guba data for a single stock.
    Decides sequentially whether to crawl main table, concatenate, get post content, and output as xlsx.

    Args:
        param: dict or tuple with at least
            - 'stock_code'
            - 'start_year'
            - 'output_suffix'
            - 'need_content'

    Returns:
        Always return True
    """
    logger = logging.getLogger("crwal_by_stkcd")
    stock_code,start_year,output_suffix,need_content=param
    if [file for file in os.listdir("finalresults") if file.startswith(stock_code) and file.endswith(output_suffix)]:
        logger.info(f"{stock_code}已完成把并输出结果，跳过")
        return True
    if stock_code not in os.listdir("respawnpoint"):
        os.mkdir(f"respawnpoint/{stock_code}")
    finished_interval=[re.search(r"\d{6}_(\d+)_\d{4}_\d+_\d+_(\d{4})_\d+_\d+\.pkl",file).groups() for file in os.listdir(f"respawnpoint/{stock_code}") if file.startswith(stock_code)]
    if not finished_interval:
        result=get_guba_table(stock_code,start_year,current_page=1)
    else:
        finished_interval=[(int(x),int(y)) for x,y in finished_interval]
        finished_interval.sort()
        if finished_interval[-1][1]>=start_year:
            page=finished_interval[-1][0]+1
            result=get_guba_table(stock_code,start_year,current_page=page)
        else:
            result=True
    if not [file for file in os.listdir("respawnpoint/afinished") if file.startswith(stock_code)]:
        generate_concated_table(stock_code)
    if need_content and not [file for file in os.listdir("respawnpoint/finished") if file.startswith(stock_code)]:
        result=get_guba_content(stock_code,continuous_fail_count=0)
    if result and output_suffix:
        if need_content:
            filename=[file for file in os.listdir("respawnpoint/finished") if file.startswith(stock_code)][0]
            df=pickle.load(open(f"respawnpoint/finished/{filename}","rb"))
        else:
            filename=[file for file in os.listdir("respawnpoint/afinished") if file.startswith(stock_code)][0]
            df=pickle.load(open(f"respawnpoint/afinished/{filename}","rb"))
        filename="".join(filename.split(".")[:-1])
        if output_suffix==".pkl":
            pickle.dump(df,open(f"finalresults/{filename}.pkl","wb"))
        elif output_suffix==".xlsx":
            outputAsXlsx(df,filename+".xlsx","finalresults")
        elif output_suffix==".csv":
            df.to_csv(f"finalresults/{filename}.csv")
    return result


if __name__=="__main__":
    start_year=2024 # 从那一年开始（按publish_date算）
    sse50_codes = [
    "600000", "600004", "600009", "600010", "600011", "600015", "600016", "600018",
    "600021", "600028", "600029", "600030", "600036", "600048", "600050", "600104",
    "600111", "600115", "600118", "600150", "600170", "600276", "600309", "600340",
    "600519", "600547", "600585", "600688", "600690", "600703", "600705", "600741",
    "600795", "600837", "600919", "600999", "601009", "601166", "601169", "601186",
    "601288", "601318", "601328", "601336", "601601", "601628", "601766", "601857",
    "601880", "601989"
    ]
    hs300_codes=['000001', '000002', '000063', '000100', '000157', '000166', '000301', '000333', '000338', '000408', '000425', '000538', '000568', '000596', '000617', '000625', '000630', '000651', '000661', '000708', '000725', '000768', '000776', '000786', '000792', '000800', '000807', '000858', '000876', '000895', '000938', '000963', '000975', '000977', '000983', '000999', '001289', '001965', '001979', '002001', '002007', '002027', '002028', '002049', '002050', '002074', '002129', '002142', '002179', '002180', '002230', '002236', '002241', '002252', '002271', '002304', '002311', '002352', '002371', '002415', '002422', '002459', '002460', '002463', '002466', '002475', '002493', '002555', '002594', '002601', '002648', '002709', '002714', '002736', '002812', '002916', '002920', '002938', '003816', '300014', '300015', '300033', '300059', '300122', '300124', '300274', '300308', '300316', '300347', '300394', '300408', '300413', '300418', '300433', '300442', '300450', '300498', '300502', '300628', '300661', '300750', '300759', '300760', '300782', '300832', '300896', '300979', '300999', '301269', '600000', '600009', '600010', '600011', '600015', '600016', '600018', '600019', '600023', '600025', '600026', '600027', '600028', '600029', '600030', '600031', '600036', '600039', '600048', '600050', '600061', '600066', '600085', '600089', '600104', '600111', '600115', '600150', '600160', '600161', '600176', '600183', '600188', '600196', '600219', '600233', '600276', '600309', '600332', '600346', '600362', '600372', '600377', '600406', '600415', '600426', '600436', '600438', '600460', '600482', '600489', '600515', '600519', '600547', '600570', '600584', '600585', '600588', '600600', '600660', '600674', '600690', '600741', '600745', '600760', '600795', '600803', '600809', '600845', '600875', '600886', '600887', '600893', '600900', '600905', '600918', '600919', '600926', '600938', '600941', '600958', '600989', '600999', '601006', '601009', '601012', '601021', '601058', '601059', '601066', '601088', '601100', '601111', '601117', '601127', '601136', '601138', '601166', '601169', '601186', '601211', '601225', '601229', '601236', '601238', '601288', '601318', '601319', '601328', '601336', '601360', '601377', '601390', '601398', '601600', '601601', '601607', '601618', '601628', '601633', '601658', '601668', '601669', '601688', '601689', '601698', '601699', '601728', '601766', '601788', '601799', '601800', '601808', '601816', '601818', '601838', '601857', '601865', '601868', '601872', '601877', '601878', '601881', '601888', '601898', '601899', '601901', '601916', '601919', '601939', '601985', '601988', '601989', '601995', '601998', '603019', '603195', '603259', '603260', '603288', '603296', '603369', '603392', '603501', '603659', '603799', '603806', '603833', '603986', '603993', '605117', '605499', '688008', '688009', '688012', '688036', '688041', '688082', '688111', '688126', '688169', '688187', '688223', '688256', '688271', '688303', '688396', '688472', '688506', '688599', '688981']
    stock_codes=sse50_codes[:6] # 股票代码（6位数字，不要后缀），列表格式
    output_suffix=".xlsx" # 输出文件后缀，可选.xlsx, .pkl, .csv
    need_content=True # 是否需要进入链接获取文章正文和ip地址
    logger = logging.getLogger("main")
    if "respawnpoint" not in os.listdir():
        logger.info(f"在工作目录{os.getcwd()}下未找到用于存储临时文件的respawnpoint文件夹，将自动创建")
        os.mkdir("respawnpoint")
    if "finalresults" not in os.listdir():
        logger.info(f"在工作目录{os.getcwd()}下未找到用于存储最终结果的finalresults文件夹，将自动创建")
        os.mkdir("finalresults")
    if "recapcha" not in os.listdir():
        os.mkdir("recapcha")
    if "afinished" not in os.listdir("respawnpoint"):
        os.mkdir("respawnpoint/afinished")
    if "finished" not in os.listdir("respawnpoint"):
        os.mkdir("respawnpoint/finished")
    pool=mp.Pool(processes=4)
    try:
        pool.imap(crwal_by_stkcd,[(stock_code,start_year,output_suffix,need_content) for stock_code in stock_codes])
    except Exception as e:
        logger.info("Caught an error:", e)
        pool.terminate()  # Optionally terminate the pool to stop further processing.
    finally:
        pool.close()
        pool.join()
    # for stock_code in stock_codes:
    #     result=crwal_by_stkcd((stock_code,start_year,output_suffix,need_content))
    logger.info("程序运行结束")
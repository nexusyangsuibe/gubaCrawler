from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.edge.options import Options
import multiprocessing as mp
import pandas as pd
import re

def create_webdriver(headless=False):
    if headless:
        options=Options()
        options.add_argument("--headless")
        options.add_experimental_option('excludeSwitches',['enable-logging'])
        options.add_argument("--log-level=3")
        driver=webdriver.Edge(options=options)
    else:
        driver=webdriver.Edge()
    with open("stealth.min.js","r") as f:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",{"source":f.read()})
    return driver

def get_guba_table(stock_code,current_page=1):
    driver=create_webdriver()
    driver.get(f"https://guba.eastmoney.com/list,{stock_code}_{current_page}.html")
    if driver.find_elements(by=By.XPATH,value="/html/body/div[4]"):
        driver.execute_script("tk_tg_zoomin()")
    if driver.title=="身份核实" or ((iframes:=driver.find_elements(by=By.CLASS_NAME,value="popwscps_d_iframe")) and iframes[0].is_displayed()):
        print("已触发人机验证，程序运行结束")
        driver.quit()
        return True
    if (bar_code:=re.search(r"\d{6}",driver.title).group(0))!=stock_code:
        print(f"已经自动转至{bar_code}股吧，程序运行结束")
        driver.quit()
        return False
    max_page_num=int(driver.find_elements(by=By.CLASS_NAME,value="nump")[-1].text)
    while True:
        try:
            article_list=driver.execute_script("return article_list")
            df=pd.DataFrame(article_list["re"])[["media_type","post_click_count","post_comment_count","post_forward_count","post_from_num","post_has_pic","post_id","post_display_time","post_last_time","post_publish_time","post_title","post_type","stockbar_code","stockbar_name","user_id","user_nickname","user_is_majia","user_extendinfos"]]
            content_navi=[node.find_element(by=By.TAG_NAME,value="a").get_property("href") for node in driver.find_element(by=By.CLASS_NAME,value="listbody").find_elements(by=By.CLASS_NAME,value="title")]
            df["link_url"]=content_navi
            if current_page<max_page_num:
                current_page+=1
                next_page_button=driver.find_element(by=By.CLASS_NAME,value="nextp")
                actions=ActionChains(driver)
                actions.move_to_element(next_page_button).perform()
                next_page_button.click()
            else:
                return False
        except:
            driver.quit()
            return get_guba_table(stock_code,current_page)

if __name__=="__main__":
    sse50_codes = [
    "600000", "600004", "600009", "600010", "600011", "600015", "600016", "600018",
    "600021", "600028", "600029", "600030", "600036", "600048", "600050", "600104",
    "600111", "600115", "600118", "600150", "600170", "600276", "600309", "600340",
    "600519", "600547", "600585", "600688", "600690", "600703", "600705", "600741",
    "600795", "600837", "600919", "600999", "601009", "601166", "601169", "601186",
    "601288", "601318", "601328", "601336", "601601", "601628", "601766", "601857",
    "601880", "601989"
    ]
    pool=mp.Pool(processes=1)
    # result=pool.map(crwal_by_stkcd,[(stock_code,start_year,output_suffix,need_content) for stock_code in stock_codes])
    try:
        for result in pool.imap_unordered(get_guba_table,sse50_codes):
            pool.terminate()
    except Exception as e:
        print("Caught an error:", e)
        pool.terminate()  # Optionally terminate the pool to stop further processing.
    finally:
        pool.close()
        pool.join()
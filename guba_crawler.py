from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from PIL import Image,ImageChops,ImageDraw
from collections.abc import Iterable
from datetime import datetime
from collections import deque
import multiprocessing as mp
from pathlib import Path
import pandas as pd
import numpy as np
import traceback
import xlsxwriter
import shutil
import pickle
import uuid
import json
import time
import os
import re

# common tool functions are as follows
def ensureCorrectPklDump(obj,filepath):
    # ensure that objects written into pickle files can be read normally to avoid corrupted writing
    fail=0
    path=Path(filepath)
    pathname=path.parent
    filename=path.name
    pickle.dump(obj,open(pathname/f"tmp_{filename}", "wb"))
    while True:
        if fail>2:
            raise RuntimeError(f"写入pickle文件已经失败了{fail}次，请检查写入对象的完整性")
        try:
            pickle.load(open(pathname/f"tmp_{filename}","rb"))
            break
        except:
            fail+=1
            pickle.dump(obj,open(pathname/f"tmp_{filename}","wb"))
    if os.path.exists(path):
        os.remove(path)
    os.rename(pathname/f"tmp_{filename}",path)
    return None

def findBestBulkNum(df,thereshold_GB,best_bulk_num=1):
    # find the best bulk number that meets the demand that all bulks smaller than thereshold_GB
    for idx in range(best_bulk_num):
        memory_usage_GB=df.iloc[int(len(df)*(idx/best_bulk_num)):int(len(df)*((idx+1)/best_bulk_num))].memory_usage(deep=True).sum()/(1024**3)
        if memory_usage_GB>thereshold_GB:
            new_bulk_num=max(int(df.memory_usage(deep=True).sum()/(1024**3))//thereshold_GB+1,best_bulk_num+1)
            return findBestBulkNum(df,thereshold_GB,best_bulk_num=new_bulk_num)
    else:
        return best_bulk_num

def outputAccording2BestBulkNum(param):
    # write into excel according to the best bulk number
    df_bulk,fileName,file_rows,thereshold_GB=param
    df_bulk=df_bulk.map(lambda x: str(x) if isinstance(x,Iterable) and not isinstance(x,str) else x)
    bulk_num=findBestBulkNum(df_bulk,thereshold_GB)
    if bulk_num==1:
        workbook=xlsxwriter.Workbook(fileName,{'constant_memory':True,"strings_to_urls":False,"nan_inf_to_errors":True})
        worksheet=workbook.add_worksheet()
        worksheet.write_row(0,0,df_bulk.columns)
        for row_idx,row in enumerate(df_bulk.itertuples(index=False),start=0):
            worksheet.write_row(row_idx+1,0,row)
        workbook.close()
    else:
        print(f"文件{fileName}所需的存储空间超过阙值{thereshold_GB}GB，再分为{bulk_num}个文件输出")
        for iidx in range(bulk_num):
            fileName_=f"{''.join(fileName.split('.')[:-1])}_{iidx}.xlsx"
            print(f"正在写入{fileName_}")
            workbook=xlsxwriter.Workbook(fileName_,{'constant_memory':True,"strings_to_urls":False,"nan_inf_to_errors":True})
            worksheet=workbook.add_worksheet()
            worksheet.write_row(0,0,df_bulk.columns)
            for row_idx,row in enumerate(df_bulk.iloc[int(file_rows*(iidx/bulk_num)):int(file_rows*((iidx+1)/bulk_num))].itertuples(index=False),start=0):
                worksheet.write_row(row_idx+1,0,row)
            workbook.close()
    return None

def outputAsXlsx(df,output_filename,output_pathname,thereshold_rows=1000000,thereshold_GB=4):
    # output the dataframe into excel with divsions within the thereshold_rows and thereshold_GB
    file_num=int(df.shape[0]//thereshold_rows)
    print(f"共{df.shape[0]}行，文件名为{output_filename}，预计分为{file_num+1}个文件输出")
    if file_num==0:
        outputAccording2BestBulkNum((df,f"{output_pathname}{'' if output_pathname.endswith('/') else '/'}{''.join(output_filename.split('.')[:-1])}.xlsx",None,thereshold_GB))
    else:
        file_rows,last_rows=divmod(df.shape[0],file_num+1)
        last_rows=file_rows+last_rows
        print(f"每个文件约有{file_rows}行")
        tasks=[]
        for idx in range(file_num):
            df_bulk=df.iloc[idx*file_rows:(idx+1)*file_rows]
            fileName=f"{output_pathname}{'' if output_pathname.endswith('/') else '/'}{''.join(output_filename.split('.')[:-1])}_{idx}.xlsx"
            tasks.append((df_bulk,fileName,file_rows,thereshold_GB))
        if last_rows:
            df_bulk=df.iloc[file_num*file_rows:]
            fileName=f"{output_pathname}{'' if output_pathname.endswith('/') else '/'}{''.join(output_filename.split('.')[:-1])}_{file_num}.xlsx"
            tasks.append((df_bulk,fileName,file_rows,thereshold_GB))
        pool=mp.Pool(processes=8)
        pool.map(outputAccording2BestBulkNum,tasks)
    return None

def create_webdriver(headless=True):
    try:
        unique_id = uuid.uuid4().hex
        base_path = os.getcwd() + "/tmpfiles"
        user_data_dir = os.path.join(base_path, f"edge_profile_{unique_id}")
        cache_dir = os.path.join(base_path, f"edge_cache_{unique_id}")
        # Ensure the directories exist
        os.makedirs(user_data_dir, exist_ok=True)
        os.makedirs(cache_dir, exist_ok=True)
        options=Options()
        options.add_experimental_option('excludeSwitches',['enable-logging'])
        options.add_argument("--log-level=3")
        options.add_argument("--inprivate")
        options.add_argument(f"user-data-dir={user_data_dir}")
        options.add_argument(f"disk-cache-dir={cache_dir}")
        if headless:
            options.add_argument("--headless")
        driver=webdriver.Edge(options=options)
        with open("stealth.min.js","r") as f:
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",{"source":f.read()})
        # Prepare your interception script as a string
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
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",{"source":xhr_intercept_script})
        return driver
    except:
        print("create_webdriver出现错误")
        traceback.print_exc()
        time.sleep(2)
        return create_webdriver(headless=headless)

def human_verify_until_success(driver, stock_code):
    while driver.title == "身份核实" or (
        (iframes := driver.find_elements(by=By.CLASS_NAME, value="popwscps_d_iframe"))
        and iframes[0].is_displayed()
    ):
        driver.execute_script("typeof tk_tg_zoomin === 'function' && tk_tg_zoomin()")
        verified = human_verification(driver, stock_code)
        if verified=="fail":
            driver.refresh()
        # else verified=="double check", just wait a second, be careful not to refresh the browser
        time.sleep(1)
    # If the verification condition is no longer met, assume success
    return True

def get_guba_table(stock_code,user_defined_start_date,current_page,update_mode=False):
    human_verify_result_this_page=None
    human_verify_result_last_page=None
    try:
        driver=create_webdriver()
        url=f"https://guba.eastmoney.com/list,{stock_code}_{current_page}.html"
        driver.get(url)
        time.sleep(1)
        driver.execute_script("typeof tk_tg_zoomin === 'function' && tk_tg_zoomin()")
        if driver.title=="身份核实" or ((iframes:=driver.find_elements(by=By.CLASS_NAME,value="popwscps_d_iframe")) and iframes[0].is_displayed()):
            print(f"{driver.current_url}触发人机验证")
            human_verify_result_this_page=human_verify_until_success(driver,stock_code)
        if stock_code not in driver.title:
            print(f"{url}已经自动转至其他股吧，休息一小时后继续")
            driver.quit()
            time.sleep(3600+np.random.normal(30,10))
            return get_guba_table(stock_code,user_defined_start_date,current_page)
        if pages:=driver.find_elements(by=By.CLASS_NAME,value="nump"):
            max_page_num=int(pages[-1].text)
        else:
            if driver.find_elements(by=By.ID,value="emptylist"):
                max_page_num=current_page
            else:
                max_page_num=current_page+1
        while True:
            if driver.title=="身份核实" or ((iframes:=driver.find_elements(by=By.CLASS_NAME,value="popwscps_d_iframe")) and iframes[0].is_displayed()):
                print(f"{driver.current_url}触发人机验证")
                human_verify_result_this_page=human_verify_until_success(driver,stock_code)
            else:
                human_verify_result_this_page=None
            article_list=driver.execute_script("return article_list")
            articles=article_list["re"]
            if articles and (list_bodies:=driver.find_elements(by=By.CLASS_NAME,value="listbody")):
                df=pd.DataFrame(article_list["re"])[["media_type","post_click_count","post_comment_count","post_forward_count","post_from_num","post_has_pic","post_id","post_display_time","post_last_time","post_publish_time","post_title","post_type","stockbar_code","stockbar_name","user_id","user_is_majia"]]
                content_navi=[node.find_element(by=By.TAG_NAME,value="a").get_property("href") for node in list_bodies[0].find_elements(by=By.CLASS_NAME,value="title")]
                df["link_url"]=content_navi
                start_date1=datetime.strptime(min(df["post_publish_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
                start_date2=datetime.strptime(min(df["post_display_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
                start_date3=datetime.strptime(min(df["post_last_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
                end_date1=datetime.strptime(max(df["post_publish_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
                end_date2=datetime.strptime(max(df["post_display_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
                end_date3=datetime.strptime(max(df["post_last_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
                start_date=sorted([start_date1,start_date2,start_date3])[-1]
                end_date=sorted([end_date1,end_date2,end_date3])[0]
                if update_mode:
                    filepath=f"respawnpoint/{stock_code}/update_tmp_folder/{stock_code}_{current_page}_{start_date}_{end_date}.pkl"
                else:
                    filepath=f"respawnpoint/{stock_code}/{stock_code}_{current_page}_{start_date}_{end_date}.pkl"
                ensureCorrectPklDump(df,filepath)
                if pages:=driver.find_elements(by=By.CLASS_NAME,value="nump"):
                    max_page_num=int(pages[-1].text) # update max_page_num because it may increase very fast when crawling
                elif driver.find_elements(by=By.ID,value="emptylist"):
                        max_page_num=current_page
            if current_page<max_page_num and start_date>=user_defined_start_date:
                current_page+=1
                if not articles:
                    url=f"https://guba.eastmoney.com/list,{stock_code}_{current_page}.html"
                    human_verify_result_last_page=human_verify_result_this_page
                    driver.get(url)
                    time.sleep(1)
                    continue
                if human_verify_result_this_page and human_verify_result_last_page:
                    print(f"{stock_code}连续触发人机验证，休息10分钟后继续")
                    time.sleep(600+np.random.normal(30,10))
                human_verify_result_last_page=human_verify_result_this_page
                next_page_button=driver.find_element(by=By.CLASS_NAME,value="nextp")
                actions=ActionChains(driver)
                actions.move_to_element(next_page_button).perform()
                try:
                    next_page_button.click()
                except ElementClickInterceptedException as e:
                    covering_element=re.search(r'Other element would receive the click: (<.+>)',e.msg).group(1)
                    # 1. Extract the tag name.
                    tag_name = re.match(r'<\s*(\w+)', covering_element).group(1)
                    # 2. Extract all attributes as (name, value) tuples.
                    # This regex matches attribute_name="attribute_value".
                    attributes = re.findall(r'(\w+)\s*=\s*"([^"]+)"', covering_element)
                    # 3. Filter out the style attribute.
                    filtered_attrs = [(name, value) for name, value in attributes if name.lower() != "style"]
                    # 4. Construct the CSS selector.
                    # For each attribute, we add [attr="value"] to the tag.
                    css_selector = tag_name
                    for attr_name, attr_value in filtered_attrs:
                        css_selector += f'[{attr_name}="{attr_value}"]'
                    driver.execute_script(f"document.querySelector('{css_selector}').style.display='none'")
                    next_page_button=driver.find_element(by=By.CLASS_NAME,value="nextp")
                    next_page_button.click()
            else:
                screen_shots=[file for file in os.listdir("recapcha") if file.startswith(f"{stock_code}_screenshot")]
                for screen_shot in screen_shots:
                    os.remove("recapcha/"+screen_shot)
                return True
    except:
        print(f"get_guba_table在处理 https://guba.eastmoney.com/list,{stock_code}_{current_page}.html 时出现错误")
        traceback.print_exc()
        time.sleep(2)
        return get_guba_table(stock_code=stock_code,user_defined_start_date=user_defined_start_date,current_page=current_page)

def human_verification(driver,stock_code):
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
    iframe=None
    iframes=driver.find_elements(by=By.CLASS_NAME,value="popwscps_d_iframe")
    if driver.title!="身份核实" and not (iframes and iframes[0].is_displayed()) and not ((divCaptcha:=driver.find_elements(by=By.ID,value="divCaptcha")) and divCaptcha[0].is_displayed()):
        return "success"
    if iframes:
        print(f"{driver.current_url}触发iframe人机验证")
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
            print("人机验证成功")
            time.sleep(2)
            if driver.title!="身份核实" and not (iframes and iframes[0].is_displayed()) and not ((divCaptcha:=driver.find_elements(by=By.ID,value="divCaptcha")) and divCaptcha[0].is_displayed()):
                return "success"
            else:
                return "double check"
        else:
            print("人机验证失败")
            driver.switch_to.default_content()
            time.sleep(1)
            return "fail"
    except:
        print("人机验证发生错误")
        driver.switch_to.default_content()
        traceback.print_exc()
        time.sleep(2)
        return "fail"

def generate_concated_table(stock_code,update_mode=False):
    if update_mode:
        df=pd.concat([pickle.load(open(f"respawnpoint/{stock_code}/update_tmp_folder/"+file,"rb")) for file in os.listdir(f"respawnpoint/{stock_code}/update_tmp_folder") if file.startswith(stock_code)],axis=0)
        original_df_filename=[f"respawnpoint/afinished/{filename}" for filename in os.listdir("respawnpoint/afinished") if filename.startswith(f"{stock_code}_afinished")][0]
        original_df=pickle.load(open(original_df_filename,"rb"))
        if "content" in original_df.columns:
            df["content"] = None
            df["ip"] = None
            df["post_user"] = None
            df["reply"] = None
        df=pd.concat([original_df,df],axis=0)
    else:
        df=pd.concat([pickle.load(open(f"respawnpoint/{stock_code}/"+file,"rb")) for file in os.listdir(f"respawnpoint/{stock_code}") if file.startswith(stock_code)],axis=0)
    df=df.drop_duplicates(subset="link_url")
    df.reset_index(inplace=True)
    df.drop("index",axis=1,inplace=True)
    start_date1=datetime.strptime(min(df["post_publish_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
    start_date2=datetime.strptime(min(df["post_display_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
    start_date3=datetime.strptime(min(df["post_last_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
    end_date1=datetime.strptime(max(df["post_publish_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
    end_date2=datetime.strptime(max(df["post_display_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
    end_date3=datetime.strptime(max(df["post_last_time"]),"%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d")
    start_date=sorted([start_date1,start_date2,start_date3])[-1]
    end_date=sorted([end_date1,end_date2,end_date3])[0]
    filepath=f"respawnpoint/afinished/{stock_code}_afinished_{start_date}_{end_date}.pkl"
    ensureCorrectPklDump(df,filepath)
    if update_mode:
        os.remove(original_df_filename)
    return True

def update2newest_by_stkcd(stock_code,need_content):
    # 数据更新仅对已经完成爬取，在respawnpoint/afinished中有临时文件的股票代码生效
    end_date_crawled=[re.search(rf"{stock_code}_afinished_\d{{4}}_\d+_\d+_(\d{{4}}_\d+_\d+)\.pkl",file).group(1) for file in os.listdir(f"respawnpoint/afinished") if file.startswith(stock_code)][0]
    if end_date_crawled==datetime.now().strftime("%Y-%m-%d"):
        print(f"{stock_code}的数据已经是最新，跳过数据更新")
        return None
    if stock_code not in os.listdir("respawnpoint"):
        os.mkdir(f"respawnpoint/{stock_code}")
    if "update_tmp_folder" in os.listdir(f"respawnpoint/{stock_code}"):
        for file in os.listdir(f"respawnpoint/{stock_code}/update_tmp_folder"):
            os.remove(f"respawnpoint/{stock_code}/update_tmp_folder/{file}")
    else:
        os.mkdir(f"respawnpoint/{stock_code}/update_tmp_folder")
    result=get_guba_table(stock_code,end_date_crawled,current_page=1,update_mode=True)
    generate_concated_table(stock_code,update_mode=True)
    shutil.rmtree(f"respawnpoint/{stock_code}/update_tmp_folder")
    if not need_content:
        print(f"{stock_code}的数据更新已完成")
    return result
    
def crwal_by_stkcd(param):
    stock_code,user_defined_start_date,update_mode,output_suffix,need_content=param
    if update_mode:
        result=update2newest_by_stkcd(stock_code,need_content)
    else:
        if [file for file in os.listdir("finalresults") if file.startswith(stock_code) and file.endswith(output_suffix)]:
            print(f"{stock_code}已完成把并输出结果，跳过")
            return True
        if stock_code not in os.listdir("respawnpoint"):
            os.mkdir(f"respawnpoint/{stock_code}")
        finished_interval=[re.search(rf"{stock_code}_(\d+)_(\d{{4}}_\d+_\d+)_\d{{4}}_\d+_\d+\.pkl",file).groups() for file in os.listdir(f"respawnpoint/{stock_code}") if file.startswith(stock_code)]
        if not finished_interval:
            result=get_guba_table(stock_code,user_defined_start_date,current_page=1)
        else:
            finished_interval=[(int(x),y) for x,y in finished_interval]
            finished_interval.sort()
            if finished_interval[-1][1]>=user_defined_start_date:
                page=finished_interval[-1][0] # sometimes there may be a funny situation, that the number of pages is decreasing rather than increasing because lots of posts are deleted
                result=get_guba_table(stock_code,user_defined_start_date,current_page=page)
            else:
                result=True
        if not [file for file in os.listdir("respawnpoint/afinished") if file.startswith(stock_code)]:
            generate_concated_table(stock_code)
    if need_content:
        if update_mode:
            result=get_guba_content(stock_code,continuous404=0)
            print(f"{stock_code}的数据更新已完成")
        else:
            if not [file for file in os.listdir("respawnpoint/finished") if file.startswith(stock_code)]:
                result=get_guba_content(stock_code,continuous404=0)
    if result and output_suffix:
        if need_content:
            filename=[file for file in os.listdir("respawnpoint/finished") if file.startswith(stock_code)][0]
            df=pickle.load(open(f"respawnpoint/finished/{filename}","rb"))
        else:
            filename=[file for file in os.listdir("respawnpoint/afinished") if file.startswith(stock_code)][0]
            df=pickle.load(open(f"respawnpoint/afinished/{filename}","rb"))
        filename="".join(filename.split(".")[:-1])
        if output_suffix==".pkl":
            filepath=f"finalresults/{filename}.pkl"
            ensureCorrectPklDump(df,filepath)
        elif output_suffix==".xlsx":
            outputAsXlsx(df,filename+".xlsx","finalresults")
        elif output_suffix==".csv":
            df.to_csv(f"finalresults/{filename}.csv")
    return result

def get_guba_content(stock_code, continuous404=0):
    url=None
    human_verify_result_this_page=None
    human_verify_result_last_page=None
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
            start_idx = df[["content", "ip", "post_user", "reply"]].last_valid_index() + 1
        
        len_df = df.shape[0]
        link_urls = df["link_url"]
        max_interval = 240

        driver = create_webdriver()
        current_idx = start_idx

        while current_idx < len_df:
            # Determine the segment boundaries
            to_rows = min(current_idx + max_interval, len_df)
            segment_range = range(current_idx, to_rows)
            
            # Initialize segment-specific storage
            contents = [None] * (to_rows - current_idx)
            ips = [None] * (to_rows - current_idx)
            post_users = [None] * (to_rows - current_idx)
            replies = [None] * (to_rows - current_idx)
            
            idx = current_idx
            while idx < to_rows:
                # Skip processing if already saved
                seg_offset = idx - current_idx
                if any([contents[seg_offset] is not None,
                        ips[seg_offset] is not None,
                        post_users[seg_offset] is not None,
                        replies[seg_offset] is not None]):
                    idx += 1
                    continue

                url = link_urls.at[idx]
                content = ip = post_user = reply = None
                
                if "guba.eastmoney.com" in url or "caifuhao.eastmoney.com" in url:
                    driver.get(url)
                    time.sleep(1)
                    driver.execute_script("typeof tk_tg_zoomin === 'function' && tk_tg_zoomin()")
                    
                    if (driver.title == "身份核实" or
                        (iframes := driver.find_elements(by=By.CLASS_NAME, value="popwscps_d_iframe")) and iframes[0].is_displayed()):
                        print(f"{driver.current_url}触发人机验证，正在爬取的股吧为{stock_code}")
                        human_verify_result_this_page=human_verify_until_success(driver, stock_code)
                    else:
                        human_verify_result_this_page=None
                    
                    if driver.find_elements(by=By.TAG_NAME, value="pre") and \
                       driver.find_element(by=By.TAG_NAME, value="pre").text == "Not Found":
                        print(f"{url} returns 404 Not Found")
                        continuous404 += 1
                    elif "guba.eastmoney.com" in url:
                        post_article = driver.execute_script(
                            'return (typeof post_article!=="undefined" && post_article) ? post_article : undefined')
                        if not post_article:
                            print(f"{url} returns 404 Not Found")
                            continuous404 += 1
                        else:
                            if "post" in post_article.keys():
                                post_article=post_article["post"]
                            content = post_article["post_content"]
                            ip = post_article["post_ip_address"] or None
                            post_user = post_article["post_user"]
                            intercepted = driver.execute_script("return window._interceptedResponses;")
                            for entry in intercepted:
                                if "reply/api/Reply/ArticleNewReplyList" in entry["url"]:
                                    r = json.loads(entry['response'])
                                    reply = r["re"] if isinstance(r["re"], list) else []
                                    break
                            continuous404 = 0
                    else:
                        content = driver.execute_script("return articleTxt")
                        ip_elements = driver.find_elements(by=By.XPATH, value='//*[@id="main"]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/span[3]')
                        ip = ip_elements[0].text.strip() if ip_elements else None
                        reply = driver.execute_script(
                            'return (typeof replyListAll!=="undefined" && replyListAll) ? replyListAll : (typeof reply_list!=="undefined") ? reply_list : undefined')
                        if reply:
                            reply = reply["re"]
                
                # Save the fetched results in the temporary storage arrays
                contents[seg_offset] = content
                ips[seg_offset] = ip
                post_users[seg_offset] = post_user
                replies[seg_offset] = reply
                
                # Handle continuous 404 scenario
                if continuous404 > 10:
                    print(f"已经连续404 Not Found {continuous404} 次，休息一小时后继续")
                    time.sleep(3600+np.random.normal(30,10))
                    # Optionally, you may want to reset continuous404 or adjust current_idx accordingly.
                    # Here, we go back a few indices if needed.
                    idx = max(current_idx, idx - continuous404)
                else:
                    idx += 1
                    if human_verify_result_this_page and human_verify_result_last_page:
                        print(f"{stock_code}连续触发人机验证，休息10分钟后继续")
                        time.sleep(600+np.random.normal(30,10))
                    human_verify_result_last_page=human_verify_result_this_page

            # Update the DataFrame with the newly collected data
            df.loc[segment_range, "content"] = contents
            df.loc[segment_range, "ip"] = ips
            df.loc[segment_range, "post_user"] = pd.Series(post_users, index=segment_range, dtype="object")
            df.loc[segment_range, "reply"] = pd.Series(replies, index=segment_range, dtype="object")
            filepath=f"respawnpoint/afinished/{filename}"
            ensureCorrectPklDump(df,filepath)
            print(f"{stock_code}已完成{to_rows}/{len_df}")
            
            # If the current segment is done, move to the next segment
            current_idx = to_rows
        
        # Once fully completed, store the finished file.
        filepath=f"respawnpoint/finished/{filename}"
        ensureCorrectPklDump(df,filepath)
        driver.quit()
        return True

    except:
        print(f"get_guba_content在处理 {url} 时出现错误")
        traceback.print_exc()
        time.sleep(2)
        # Optionally decide whether to break out of the loop or continue
        return get_guba_content(stock_code, continuous404=continuous404)


if __name__=="__main__":
    user_defined_start_date="2025-08-31" # 请以YYYY-MM-DD格式输入，从那一天开始（按publish_date, display_date, last_date孰高计算，三者都低于user_defined_start_date时爬取结束）
    hs300_codes=['000001', '000002', '000063', '000100', '000157', '000166', '000301', '000333', '000338', '000408', '000425', '000538', '000568', '000596', '000617', '000625', '000630', '000651', '000661', '000708', '000725', '000768', '000776', '000786', '000792', '000800', '000807', '000858', '000876', '000895', '000938', '000963', '000975', '000977', '000983', '000999', '001289', '001965', '001979', '002001', '002007', '002027', '002028', '002049', '002050', '002074', '002129', '002142', '002179', '002180', '002230', '002236', '002241', '002252', '002271', '002304', '002311', '002352', '002371', '002415', '002422', '002459', '002460', '002463', '002466', '002475', '002493', '002555', '002594', '002601', '002648', '002709', '002714', '002736', '002812', '002916', '002920', '002938', '003816', '300014', '300015', '300033', '300059', '300122', '300124', '300274', '300308', '300316', '300347', '300394', '300408', '300413', '300418', '300433', '300442', '300450', '300498', '300502', '300628', '300661', '300750', '300759', '300760', '300782', '300832', '300896', '300979', '300999', '301269', '600000', '600009', '600010', '600011', '600015', '600016', '600018', '600019', '600023', '600025', '600026', '600027', '600028', '600029', '600030', '600031', '600036', '600039', '600048', '600050', '600061', '600066', '600085', '600089', '600104', '600111', '600115', '600150', '600160', '600161', '600176', '600183', '600188', '600196', '600219', '600233', '600276', '600309', '600332', '600346', '600362', '600372', '600377', '600406', '600415', '600426', '600436', '600438', '600460', '600482', '600489', '600515', '600519', '600547', '600570', '600584', '600585', '600588', '600600', '600660', '600674', '600690', '600741', '600745', '600760', '600795', '600803', '600809', '600845', '600875', '600886', '600887', '600893', '600900', '600905', '600918', '600919', '600926', '600938', '600941', '600958', '600989', '600999', '601006', '601009', '601012', '601021', '601058', '601059', '601066', '601088', '601100', '601111', '601117', '601127', '601136', '601138', '601166', '601169', '601186', '601211', '601225', '601229', '601236', '601238', '601288', '601318', '601319', '601328', '601336', '601360', '601377', '601390', '601398', '601600', '601601', '601607', '601618', '601628', '601633', '601658', '601668', '601669', '601688', '601689', '601698', '601699', '601728', '601766', '601788', '601799', '601800', '601808', '601816', '601818', '601838', '601857', '601865', '601868', '601872', '601877', '601878', '601881', '601888', '601898', '601899', '601901', '601916', '601919', '601939', '601985', '601988', '601989', '601995', '601998', '603019', '603195', '603259', '603260', '603288', '603296', '603369', '603392', '603501', '603659', '603799', '603806', '603833', '603986', '603993', '605117', '605499', '688008', '688009', '688012', '688036', '688041', '688082', '688111', '688126', '688169', '688187', '688223', '688256', '688271', '688303', '688396', '688472', '688506', '688599', '688981']
    stock_codes=hs300_codes # 股票代码（6位数字，不要后缀），列表格式
    output_suffix=".xlsx" # 输出文件后缀，可选.xlsx, .pkl, .csv
    need_content=True # 是否需要进入链接获取文章正文和ip地址
    update_mode=False # 是否使用更新模式，更新模式可以将已经完成爬取的数据更新到最新，仅对已经完成爬取、在respawnpoint/afinished文件夹中有临时文件的股票代码生效
    if "respawnpoint" not in os.listdir():
        print(f"在工作目录{os.getcwd()}下未找到用于存储临时文件的respawnpoint文件夹，将自动创建")
        os.mkdir("respawnpoint")
    if "finalresults" not in os.listdir():
        print(f"在工作目录{os.getcwd()}下未找到用于存储最终结果的finalresults文件夹，将自动创建")
        os.mkdir("finalresults")
    if "recapcha" not in os.listdir():
        os.mkdir("recapcha")
    Path("tmpfiles").mkdir(exist_ok=True)
    if "afinished" not in os.listdir("respawnpoint"):
        os.mkdir("respawnpoint/afinished")
    if "finished" not in os.listdir("respawnpoint"):
        os.mkdir("respawnpoint/finished")
    if update_mode:
        if [file for file in os.listdir("respawnpoint/afinished")]:
            stock_codes_afinished=[re.search(r"(.+)_afinished_\d{4}_\d+_\d+_\d{4}_\d+_\d+\.pkl",file).group(1) for file in os.listdir(f"respawnpoint/afinished")]
            stock_codes=[stock_code for stock_code in stock_codes_afinished if stock_code in stock_codes]
        else:
            stock_codes=[]
        if not stock_codes:
            raise RuntimeError("在respawnpoint/afinished文件夹内没有发现符合要求的已经完成爬取的股票代码，无法使用更新模式")
        print(f"可以执行数据更新操作的股票代码为{stock_codes}")
    user_defined_start_date=user_defined_start_date.replace("-","_")
    # multiprocessing
    pool=mp.Pool(processes=2)
    try:
        pool.imap(crwal_by_stkcd,[(stock_code,user_defined_start_date,update_mode,output_suffix,need_content) for stock_code in stock_codes])
    except Exception as e:
        print("Caught an error:", e)
        pool.terminate()  # Optionally terminate the pool to stop further processing.
    finally:
        pool.close()
        pool.join()
    # debug
    # for stock_code in stock_codes:
    #     result=crwal_by_stkcd((stock_code,user_defined_start_date,update_mode,output_suffix,need_content))
    if "tmpfiles" in os.listdir():
        shutil.rmtree("tmpfiles")
    print("程序运行结束")
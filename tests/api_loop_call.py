import psycopg2
import requests
import logging
import json

# 配置日志记录
logging.basicConfig(filename='api_call.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 配置 PostgreSQL 连接信息
host = '10.52.200.1'
database = 'HOSPITAL_HDW'
user = 'gpadmin'
password = 'wn@123'

# 建立 PostgreSQL 连接
try:
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    cursor = conn.cursor()
    logging.info('成功连接到 PostgreSQL')
    # 从相应表中获取 patid，假设表名仍为 mz_brxxk，可按需修改
    cursor.execute("select   person_id ,full_name ,PUBLIC_AT,* from  MEDTECH_RIS_REPORT  where  encounter_type_no ='1'  order by PUBLIC_AT  desc  limit 100")
    patids = [row[0] for row in cursor.fetchall()]

except psycopg2.Error as e:
    error_msg = f"PostgreSQL 连接错误: {e}"
    print(error_msg)
    logging.error(error_msg)
    exit(1)

# 配置接口信息
base_url = 'http://10.51.28.117:7080/datafactory/insertHealthPortrait'
params = {
    'jzlb': 1,
    'visitStartTimeGt': '2025-01-01'
}

# 初始化计数器
valid_records_count = 0

# 循环调用接口
for patid in patids:
    params['patId'] = patid
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            response_data = json.loads(response.text)
            success_msg = f"成功调用接口，patid: {patid}，响应内容: {response.text}"
            print(success_msg)
            logging.info(success_msg)
            
            # 检查响应中的病历条数
            if response_data.get('msg') and '查询病历条数:' in response_data['msg']:
                record_count = int(response_data['msg'].split(':')[1])
                if record_count > 0:
                    valid_records_count += 1
        else:
            fail_msg = f"调用接口失败，patid: {patid}，状态码: {response.status_code}，响应内容: {response.text}"
            print(fail_msg)
            logging.warning(fail_msg)
    except requests.RequestException as e:
        request_error_msg = f"请求发生错误，patid: {patid}，错误信息: {e}"
        print(request_error_msg)
        logging.error(request_error_msg)

# 打印统计结果
print(f"\n本次循环中返回查询病历条数大于0的总人数：{valid_records_count}")
logging.info(f"本次循环中返回查询病历条数大于0的总人数：{valid_records_count}")

# 关闭 PostgreSQL 连接
conn.close()
logging.info('关闭 PostgreSQL 连接')
    
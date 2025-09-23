# 健康画像ETL项目

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Neo4j](https://img.shields.io/badge/Neo4j-5.0+-green.svg)
![Flask](https://img.shields.io/badge/Flask-2.3+-red.svg)
![SQL Server](https://img.shields.io/badge/SQL%20Server-2019+-orange.svg)

一个基于Python + Neo4j的健康画像数据ETL工程，从SQL Server获取患者ID列表，调用大数据平台API获取患者健康画像数据，经过处理后构建图谱存储到Neo4j图数据库中，并提供RESTful API服务。支持增量更新、批量处理、错误重试和定时调度等企业级功能。

## 🏗️ 项目架构

```
etl_neo4j/
├── main.py                    # 🎯 主入口：ETL单次执行
├── app.py                     # 🌐 Web API服务入口  
├── check_config.py            # ✅ 配置验证工具
├── config/                    # ⚙️ 配置管理
│   ├── settings.py           # 数据库、API、调度配置
│   └── etl_state.json        # ETL增量状态追踪
├── etl/                      # 🔄 ETL核心模块
│   ├── core/
│   │   └── etl_patient.py    # 患者数据处理核心逻辑
│   ├── processors/
│   │   └── health_portrait.py # 健康画像处理器
│   └── utils/                # 🛠️ 工具类
│       ├── api.py           # 大数据平台API调用封装
│       ├── db.py            # Neo4j连接管理(线程安全)
│       ├── sqlserver.py     # SQL Server连接(获取患者ID)
│       └── logger.py        # 日志管理(防重复初始化)
├── scheduler/               # 📅 调度管理
│   ├── job_manager.py      # 作业管理器(批量处理)
│   └── scheduler.py        # 定时调度器
├── files/                  # 📄 测试数据文件
├── templates/              # 🎨 HTML模板
├── static/                 # 📦 静态资源
├── tests/                  # 🧪 测试文件
├── logs/                   # 📋 日志文件
├── archive/                # 📦 归档文件(已清理)
├── start.sh / start.bat    # 🚀 ETL启动脚本
└── scheduler_start.sh/bat  # ⏰ 定时调度启动脚本
```

## 🔄 数据流程

```mermaid
graph TD
    A[SQL Server<br/>ai_patients表] --> B[main.py<br/>获取患者ID列表]
    B --> C[JobManager<br/>批量处理]
    C --> D[大数据平台API<br/>获取健康画像数据]
    D --> E[HealthPortraitProcessor<br/>数据处理验证]
    E --> F[etl_patient.py<br/>图谱构建与存储]
    F --> G[Neo4j图数据库<br/>健康知识图谱]
    G --> H[app.py<br/>RESTful API服务]
    H --> I[前端应用<br/>健康画像展示]
    
    J[scheduler.py<br/>定时调度] -.-> B
    K[check_config.py<br/>配置验证] -.-> B
    
    style A fill:#e1f5fe
    style G fill:#c8e6c9
    style I fill:#fff3e0

## 🚀 快速开始

### 环境要求

- **Python 3.8+** - 支持类型提示和f-strings
- **Neo4j 5.0+** - 图数据库（支持APOC插件）
- **SQL Server 2019+** - 存储患者ID列表的ai_patients表
- **大数据平台API访问权限** - 获取健康画像数据

### 安装依赖

```bash
# 安装Python依赖
pip install -r requirements.txt

# 或使用虚拟环境(推荐)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### 配置文件

修改 `config/settings.py` 中的配置：

```python
# Neo4j连接配置
NEO4J_URI = "bolt://neo4j.haxm.local:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "your_password"
NEO4J_DATABASE = "neo4j"

# 大数据平台API配置
BIGDATA_API_BASE_URL = "http://your-api-server:port"
BIGDATA_API_TIMEOUT = 10000

# SQL Server配置 (患者ID来源)
SQL_HOST = "10.52.8.78"
SQL_DATABASE = "health_portrait"
SQL_USER = "health_portrait_user"
SQL_PASSWORD = "your_password"

# 调度配置
BATCH_SIZE = 50          # 批处理大小
MAX_WORKERS = 1          # 并发线程数(避免死锁)
RETRY_TIMES = 3          # 重试次数
RETRY_DELAY = 5          # 重试延迟(秒)
```

### 验证配置

```bash
# 验证配置是否正确
python check_config.py

# 或使用启动脚本选项
start.bat config  # Windows
./start.sh config # Linux/Mac
```

### 运行ETL任务

```bash
# 执行单次ETL数据处理 (增量更新)
python main.py

# 或使用启动脚本(推荐，包含配置验证)
./start.sh          # Linux/Mac
start.bat           # Windows

# 使用特定参数
start.bat run       # 直接运行
start.bat config    # 验证配置
start.bat help      # 显示帮助
```

### 启动定时调度

```bash
# 启动ETL定时调度器 (默认24小时一次)
./scheduler_start.sh      # Linux/Mac
scheduler_start.bat       # Windows

# 或使用Python直接调用
python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.start(24)"

# 自定义调度间隔(小时)
python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.start(12)"  # 12小时
```

### 启动API服务

```bash
# 启动Web API服务
python app.py

# 或使用Flask开发服务器
flask --app app.py run --host=0.0.0.0 --port=5000
```

服务启动后，访问 `http://localhost:5000/api/docs` 查看API文档。

## 📊 核心功能

### 🔄 ETL数据处理

- **增量更新**：基于时间戳的增量数据处理，避免重复处理
- **批量处理**：支持多线程批量处理患者数据，可配置批大小
- **容错机制**：完善的错误处理和可配置重试策略
- **状态追踪**：持久化ETL执行状态，支持断点续传
- **日志管理**：结构化日志记录，支持文件轮转和级别过滤
- **配置验证**：内置配置验证工具，快速诊断环境问题

#### 执行模式

1. **单次执行**：通过`main.py`或`start.sh`执行一次ETL任务
2. **定时调度**：通过`scheduler_start.sh`启动持续的定时ETL任务

### 🕸️ 图谱构建

基于健康领域本体，构建包含以下实体和关系的知识图谱：

#### 核心实体
- **患者实体**：基础信息、证件信息、婚育状况
- **就诊记录**：门诊/住院/体检，关联医院、科室、医生  
- **医疗数据**：诊断、检查、检验、处方等
- **家族关系**：配偶、子女、父母关系图谱
- **生活方式**：吸烟史、饮酒史、睡眠评估等

#### 关系类型
- **HAD_ENCOUNTER**: 患者-就诊关系
- **RECORDED_DIAGNOSIS**: 就诊-诊断关系  
- **SPOUSE_OF**: 配偶关系
- **PARENT_OF**: 父子关系
- **HAS_ALLERGY_TO**: 过敏关系
- **HAS_FAMILY_HISTORY**: 家族史关系

### 🌐 API服务

提供完整的RESTful API接口，支持健康画像数据的查询和展示：

| 接口 | 描述 | 方法 |
|------|------|------|
| `/api/patients/{id}/dashboard` | 患者仪表盘概览 | GET |
| `/api/patients/{id}/encounters` | 就诊记录列表 | GET |
| `/api/patients/{id}/history/medical` | 既往医疗史 | GET |
| `/api/patients/{id}/history/family` | 家族史 | GET |
| `/api/patients/{id}/allergies` | 过敏史 | GET |
| `/api/patients/{id}/family-graph` | 家族关系图谱 | GET |
| `/api/docs` | API接口文档 | GET |

## ⚙️ 配置说明

### 调度配置

```python
# 批处理配置
BATCH_SIZE = 50          # 批处理大小(推荐 50-100)
MAX_WORKERS = 1          # 并发线程数(暂时串行，避免死锁)

# 重试配置  
RETRY_TIMES = 3          # 重试次数
RETRY_DELAY = 5          # 重试延迟(秒)

# 超时配置
CONNECTION_TIMEOUT = 30  # 数据库连接超时
QUERY_TIMEOUT = 300      # 查询超时(5分钟)
BIGDATA_API_TIMEOUT = 10000  # API调用超时
```

### 日志配置

```python
LOG_DIR = "logs"         # 日志目录(使用绝对路径)
LOG_LEVEL = "INFO"       # 日志级别(DEBUG/INFO/WARNING/ERROR)
LOG_FILE_ENCODING = "utf-8"  # 日志文件编码
LOG_MAX_BYTES = 10 * 1024 * 1024  # 日志文件大小限制(10MB)
LOG_BACKUP_COUNT = 5     # 保留旧日志文件数量
```

### 数据库配置

```python
# Neo4j配置
NEO4J_URI = "bolt://neo4j.haxm.local:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Weohgust_2025!"
NEO4J_DATABASE = "neo4j"

# SQL Server配置(患者ID来源)
SQL_HOST = "10.52.8.78"
SQL_PORT = "1433"
SQL_DATABASE = "health_portrait"
SQL_USER = "health_portrait_user"
SQL_PASSWORD = "Yiwenbhu_2025!"
SQL_AI_PATIENTS_TABLE = "ai_patients"
SQL_PATIENT_ID_COLUMN = "patient_id"
SQL_UPDATE_TIME_COLUMN = "update_time"
```

## 🗄️ 数据模型

### 核心节点类型

- **Patient**: 患者节点
- **Encounter**: 就诊记录
- **Condition**: 疾病/诊断
- **Hospital**: 医院
- **Department**: 科室
- **Provider**: 医生
- **LabTestReport**: 检验报告
- **Examination**: 检查记录
- **Allergen**: 过敏原
- **LifestyleFact**: 生活方式事实

### 关系类型

- **HAD_ENCOUNTER**: 患者-就诊关系
- **RECORDED_DIAGNOSIS**: 就诊-诊断关系
- **SPOUSE_OF**: 配偶关系
- **PARENT_OF**: 父子关系
- **HAS_ALLERGY_TO**: 过敏关系
- **HAS_FAMILY_HISTORY**: 家族史关系

## 📁 文件说明

### 核心文件

- `main.py`: ETL单次执行入口，支持增量更新和错误重试
- `app.py`: Web API服务入口，提供健康画像数据查询接口
- `check_config.py`: 配置验证工具，验证环境配置和模块导入
- `config/settings.py`: 项目配置文件，包含验证方法
- `start.sh/start.bat`: 项目启动脚本，支持多种模式
- `scheduler_start.sh/scheduler_start.bat`: 定时调度启动脚本
- `requirements.txt`: Python依赖包列表

### 模块说明

#### `etl/core/`
- `etl_patient.py`: 患者数据处理核心逻辑，包含数据转换和图谱构建

#### `etl/processors/`
- `health_portrait.py`: 健康画像数据处理器，负责协调API调用和数据验证

#### `etl/utils/`
- `api.py`: 大数据平台API调用封装，支持重试和超时处理
- `db.py`: Neo4j连接管理，线程安全的单例模式
- `sqlserver.py`: SQL Server连接管理，获取患者ID列表
- `logger.py`: 日志管理，防重复初始化，支持文件轮转

#### `scheduler/`
- `job_manager.py`: 作业管理器，负责批量处理和错误队列管理
- `scheduler.py`: 定时调度器，支持自定义调度间隔

### 其他文件夹

- `files/`: 测试数据文件和样例数据
- `templates/`: Flask HTML模板文件
- `static/`: 静态资源文件(CSS, JS, 图片等)
- `tests/`: 单元测试和集成测试文件
- `logs/`: 日志文件存储目录，自动轮转

### 归档文件

`archive/` 目录包含项目开发过程中的历史文件和测试文件：
- 旧版本的代码文件
- 测试数据样例
- 开发过程中的临时文件
- 备份配置和历史版本

## 🔧 开发指南

### 代码规范

- 使用Black进行代码格式化：`black .`
- 使用Flake8进行代码检查：`flake8 .`
- 遵循PEP 8编码规范
- 使用类型提示(Type Hints)提高代码可读性
- 每个模块都要包含适当的文档字符串

### 测试

```bash
# 运行所有测试
python -m pytest tests/ -v

# 运行单个测试文件
python -m pytest tests/test_specific.py -v

# 运行配置验证测试
python test_code_quality.py
python test_sql_connection.py

# 覆盖率测试
python -m pytest tests/ --cov=etl --cov-report=html
```

### 开发环境搭建

```bash
# 1. 克隆项目
git clone <repository_url>
cd etl_neo4j

# 2. 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# 3. 安装依赖
pip install -r requirements.txt

# 4. 配置环境
cp config/settings.py.example config/settings.py
# 编辑 config/settings.py 修改配置

# 5. 验证环境
python check_config.py
```

### 调试技巧

1. **增加日志级别**：在`config/settings.py`中设置`LOG_LEVEL = "DEBUG"`
2. **单步调试**：使用`python -m pdb main.py`进行单步调试
3. **小批量测试**：设置`BATCH_SIZE = 1`进行单个患者测试
4. **API调试**：使用Postman或curl测试API接口

### Git工作流

```bash
# 创建功能分支
git checkout -b feature/new-feature

# 提交修改
git add .
git commit -m "feat: add new feature"

# 推送到远程仓库
git push origin feature/new-feature

# 创建 Pull Request
```

## 📈 监控与维护

### 状态监控

- **ETL状态文件**：`config/etl_state.json` - 记录上次成功执行的时间戳
- **日志监控**：查看 `logs/` 目录下的日志文件
  - `main.log`: 主程序日志
  - `api.log`: API调用日志  
  - `health_portrait.log`: 数据处理日志
- **Neo4j数据库监控**：通过Neo4j Browser查看数据状态
- **API健康检查**：访问 `/api/docs` 检查服务状态

### 性能监控

```bash
# 查看处理速度
grep "处理第" logs/main.log | tail -10

# 查看错误统计
grep "ERROR" logs/main.log | wc -l

# 查看重试情况
grep "重试" logs/main.log | tail -5

# 查看Neo4j连接状态
grep "Neo4j" logs/main.log | tail -5
```

### 定期维护任务

1. **日志清理**：定期清理过期日志文件(自动轮转)
2. **数据库维护**：定期备份Neo4j数据库
3. **配置检查**：定期运行`check_config.py`验证配置
4. **依赖更新**：定期检查和更新Python依赖包

### 常见问题

#### 1. 连接超时
```bash
# 检查网络连接
telnet neo4j.haxm.local 7687
telnet 10.52.8.78 1433

# 调整超时配置
# 在 config/settings.py 中增加：
CONNECTION_TIMEOUT = 60
QUERY_TIMEOUT = 600
```

#### 2. 内存不足
```bash
# 调整批处理大小
BATCH_SIZE = 20  # 降低批大小

# 清理Neo4j缓存
CALL apoc.util.validate(apoc.node.degree.in() > 1000, "Node has too many relationships", [0]);
```

#### 3. 数据重复
```bash
# 检查唯一性约束
CREATE CONSTRAINT unique_patient_id IF NOT EXISTS FOR (p:Patient) REQUIRE p.patientId IS UNIQUE;

# 清理重复数据
MATCH (p:Patient) WITH p.patientId as id, collect(p) as nodes WHERE size(nodes) > 1 
WITH nodes[1..] as duplicates UNWIND duplicates as dup DETACH DELETE dup;
```

#### 4. API响应慢
```bash
# 增加数据库索引
CREATE INDEX patient_id_index IF NOT EXISTS FOR (p:Patient) ON (p.patientId);
CREATE INDEX encounter_date_index IF NOT EXISTS FOR (e:Encounter) ON (e.visitStartTime);

# 优化查询
# 使用分页查询，限制返回结果数量
```

#### 5. 日志中文乱码
```bash
# 检查系统编码
echo $LANG  # Linux/Mac
chcp       # Windows

# 设置环境变量
export PYTHONIOENCODING=utf-8  # Linux/Mac
set PYTHONIOENCODING=utf-8     # Windows
```

### 故障排查流程

1. **检查日志**：查看最新的错误日志
2. **验证配置**：运行`python check_config.py`
3. **测试连接**：检查数据库和API连接
4. **检查资源**：监控CPU、内存和磁盘使用情况
5. **重启服务**：必要时重启相关服务

## 🤝 贡献

欢迎提交Issue和Pull Request来改进项目。

### 贡献指南

1. Fork 项目到您的GitHub账户
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的修改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建Pull Request

### 贡献类型

- 🐛 Bug修复
- ✨ 新功能开发
- 📆 文档完善
- 🎨 代码优化
- 🧪 测试用例添加
- 🔧 配置优化

## 📄 更新日志

### v2.0.0 (2025-01-15)
- ✨ 新增配置验证工具
- 🐛 修复日志重复初始化问题
- 🔒 增加线程安全的Neo4j连接
- ⚡ 优化重试机制和错误处理
- 📆 完善项目文档和代码注释
- 🧪 增加单元测试和集成测试

### v1.0.0 (2024-12-01)
- 🎉 初始版本发布
- 🔄 基本 ETL 功能实现
- 🌐 Web API 服务
- 📅 定时调度功能

## 📄 许可证

本项目采用MIT许可证。详细信息请参见 [LICENSE](LICENSE) 文件。

## 📞 联系方式

- **项目作者**: 健康画像开发团队
- **项目仓库**: [GitHub Repository](https://github.com/your-org/etl_neo4j)
- **问题反馈**: [GitHub Issues](https://github.com/your-org/etl_neo4j/issues)
- **技术支持**: 请通过GitHub Issues提交技术问题

---

❤️ **感谢使用健康画像ETL项目！** 

💬 如有问题或建议，请随时联系开发团队。
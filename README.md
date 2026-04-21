# MCP Server for Dameng DM8

达梦数据库 DM8 的 MCP (Model Context Protocol) 服务器实现，为 AI 助手提供数据库查询和结构发现能力。

## 功能特性

| 类型       | 名称                | 功能                      |
|----------|-------------------|-------------------------|
| Tool     | dm_query          | 执行只读 SQL 查询（支持行数限制）     |
| Tool     | dm_list_tables    | 列出所有表（支持包含视图）           |
| Tool     | dm_describe_table | 查看表完整结构（列/主键/索引/**外键**） |
| Tool     | dm_sample_data    | 查看表前 N 行数据样本            |
| Resource | schema://tables   | **自动发现**所有表和视图          |
| Resource | table://{name}    | **自动发现**单个表结构           |

## 配置

### .mcp.json 文件

在项目根目录创建 `.mcp.json` 文件：

```json
{
	"mcpServers":{
		"dameng":{
			"command":"npx",
			"args":[
				"-y",
				"mcp-server-dm8"
			],
			"env":{
				"DM_HOST":"192.168.1.19",
				"DM_PORT":"5237",
				"DM_USER":"DATA_SMP_USER",
				"DM_PASSWORD":"your_password",
				"DM_SCHEMA":"DATA_SMP_USER"
			}
		}
	}
}
```

## 环境变量

| 变量          | 说明      | 默认值       | 必填    |
|-------------|---------|-----------|-------|
| DM_HOST     | 达梦数据库地址 | localhost | 否     |
| DM_PORT     | 达梦数据库端口 | 5237      | 否     |
| DM_USER     | 数据库用户名  | SYSDBA    | 否     |
| DM_PASSWORD | 数据库密码   | -         | **是** |
| DM_SCHEMA   | 数据库模式   | DM_USER   | 否     |

## 工具说明

### dm_query

执行只读 SQL 查询。

**参数：**

- `sql` - SQL 语句（仅支持 SELECT/SHOW/DESC/DESCRIBE/EXPLAIN/WITH）
- `max_rows` - 最大返回行数（默认 1000，超出自动截断）

**返回：** rowCount、totalRowCount、isTruncated、fields、rows

### dm_list_tables

列出所有表。

**参数：**

- `include_views` - 是否包含视图（默认 false）

**返回：** totalCount、tables [{name, type, comments}]

### dm_describe_table

查看表完整结构。

**参数：** `table_name` - 表名（字母/数字/下划线/横线）

**返回：** tableName、tableComments、columns、primaryKeys、indexes、**foreignKeys**

### dm_sample_data

查看表数据样本。

**参数：**

- `table_name` - 表名
- `limit` - 行数（1-1000，默认 10）

**返回：** tableName、rowCount、fields、rows

## Resource（自动发现）

AI 可以**自动读取**以下资源，无需显式调用工具：

- **schema://tables** — 获取所有表和视图的完整列表
- **table://表名** — 获取单个表的完整结构（含外键关系）

## 安全特性

- SQL 注入防护 - 表名正则验证 + 危险关键字拦截
- 只读限制 - 仅允许 SELECT/SHOW/DESC/EXPLAIN/WITH
- 多语句防护 - 禁止包含分号（;）
- 行数限制 - dm_query 和 dm_sample_data 最大 1000 行
- 密码校验 - 启动时强制检查 DM_PASSWORD
- 连接池管理 - 自动释放连接
# MCP Server for Dameng DM8

达梦数据库 DM8 的 MCP (Model Context Protocol) 服务器实现，为 Claude Code 提供数据库查询能力。

## 功能特性

| 工具                | 功能                                                  |
|-------------------|-----------------------------------------------------|
| dm_query          | 执行只读 SQL 查询（SELECT/SHOW/DESC/DESCRIBE/EXPLAIN/WITH） |
| dm_list_tables    | 列出当前用户有权限访问的所有表                                     |
| dm_describe_table | 查看指定表的完整结构（列、主键、索引）                                 |
| dm_sample_data    | 查看指定表的前 N 行数据样本                                     |

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
				"DM_USER":"SYSDBA",
				"DM_PASSWORD":"your_password",
				"DM_SCHEMA":"SYSDBA"
			}
		}
	}
}
```

## 环境变量

| 变量          | 说明      | 默认值       |
|-------------|---------|-----------|
| DM_HOST     | 达梦数据库地址 | localhost |
| DM_PORT     | 达梦数据库端口 | 5237      |
| DM_USER     | 数据库用户名  | SYSDBA    |
| DM_PASSWORD | 数据库密码   | -         |
| DM_SCHEMA   | 数据库模式   | DM_USER   |

## 使用示例

```
用户: 请查询 plan_work_date 表的结构和数据
Claude: [调用 dm_describe_table 和 dm_sample_data]

用户: 列出数据库中所有表
Claude: [调用 dm_list_tables]

用户: 执行 SELECT * FROM user WHERE status = 1
Claude: [调用 dm_query]
```

## 工具说明

### dm_query

执行只读 SQL 查询。

**参数：** `sql` - SQL 语句（仅支持 SELECT/SHOW/DESC/DESCRIBE/EXPLAIN/WITH）

**返回：** rowCount、fields、rows

### dm_list_tables

列出所有表。

**参数：** 无

**返回：** totalCount、tables [{name, comments}]

### dm_describe_table

查看表结构。

**参数：** `table_name` - 表名（字母/数字/下划线/横线）

**返回：** tableName、tableComments、columns、primaryKeys、indexes

### dm_sample_data

查看表数据样本。

**参数：**

- `table_name` - 表名
- `limit` - 行数（1-1000，默认 10）

**返回：** tableName、rowCount、fields、rows

## 安全特性

- SQL 注入防护 - 表名正则验证
- 只读限制 - 仅允许 SELECT/SHOW/DESC/EXPLAIN/WITH
- 行数限制 - dm_sample_data 最大 1000 行
- 连接池管理 - 自动释放连接
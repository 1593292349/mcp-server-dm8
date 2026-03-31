import 'dotenv/config';
import {McpServer} from '@modelcontextprotocol/sdk/server/mcp.js';
import {StdioServerTransport} from '@modelcontextprotocol/sdk/server/stdio.js';
import {z} from 'zod';
import dmdb from 'dmdb';

// 达梦数据库配置
const DM_CONFIG = {
	host:process.env.DM_HOST || 'localhost',
	port:parseInt(process.env.DM_PORT || '5236', 10),
	user:process.env.DM_USER || 'SYSDBA',
	password:process.env.DM_PASSWORD || '',
	schema:process.env.DM_SCHEMA || process.env.DM_USER || 'SYSDBA',
};

// 连接池
let pool = null;

// 获取连接池
async function getPool(){
	if(!pool){
		pool = await dmdb.createPool(DM_CONFIG);
	}
	return pool;
}

// 执行查询
async function executeQuery(sql, params = []){
	const pool = await getPool();
	const conn = await pool.getConnection();
	try{
		const result = await conn.execute(sql, params);
		return result;
	}finally{
		await conn.close();
	}
}

// 创建 MCP 服务器
const server = new McpServer(
		{name:'dameng-mcp', version:'1.0.0'},
		{capabilities:{tools:{}}},
);

// ==========================================
// 工具：执行 SQL 查询
// ==========================================
server.tool(
		'dm_query',
		'执行只读SQL查询（达梦数据库）',
		{
			sql:z.string().describe('SQL查询语句（仅支持 SELECT、SHOW、DESC、EXPLAIN）'),
		},
		async({sql}) => {
			try{
				// 安全检查：只允许只读SQL
				const sqlUpper = sql.trim().toUpperCase();
				const readOnlyPatterns = /^(SELECT|SHOW|DESC|DESCRIBE|EXPLAIN|WITH)\s/i;

				if(!readOnlyPatterns.test(sqlUpper)){
					return {
						content:[{type:'text', text:'错误：仅允许执行只读SQL（SELECT、SHOW、DESC、EXPLAIN）'}],
						isError:true,
					};
				}

				const result = await executeQuery(sql);

				// 格式化结果
				const rows = result.rows || result.ResultSet || [];
				const fields = result.metaData || [];

				// 转换为更友好的格式
				const formattedRows = rows.map(row => {
					const obj = {};
					if(Array.isArray(row)){
						fields.forEach((field, index) => {
							obj[field.name || field.NAME || `col_${index}`] = row[index];
						});
					}else{
						Object.assign(obj, row);
					}
					return obj;
				});

				return {
					content:[
						{
							type:'text',
							text:JSON.stringify({
								success:true,
								rowCount:formattedRows.length,
								fields:fields.map(f => f.name || f.NAME),
								rows:formattedRows,
							}, null, 2),
						},
					],
				};
			}catch(err){
				return {
					content:[{type:'text', text:`错误：${err.message}`}],
					isError:true,
				};
			}
		},
);

// ==========================================
// 工具：列出所有表
// ==========================================
server.tool(
		'dm_list_tables',
		'列出当前用户的所有表',
		{},
		async() => {
			try{
				const sql = `
                    SELECT TABLE_NAME as tableName,
                           COMMENTS   as comments
                    FROM USER_TAB_COMMENTS
                    WHERE TABLE_TYPE = 'TABLE'
                    ORDER BY TABLE_NAME
				`;
				const result = await executeQuery(sql);
				const rows = result.rows || [];

				return {
					content:[
						{
							type:'text',
							text:JSON.stringify({
								success:true,
								tables:rows.map(row => (
										{
											name:row[0] || row.TABLENAME,
											comments:row[1] || row.COMMENTS || '',
										}
								)),
							}, null, 2),
						},
					],
				};
			}catch(err){
				return {
					content:[{type:'text', text:`错误：${err.message}`}],
					isError:true,
				};
			}
		},
);

// ==========================================
// 工具：查看表结构
// ==========================================
server.tool(
		'dm_describe_table',
		'查看指定表的结构',
		{
			table_name:z.string().describe('表名称'),
		},
		async({table_name}) => {
			try{
				// 查询列信息
				const columnSql = `
                    SELECT COLUMN_NAME  as columnName,
                           DATA_TYPE    as dataType,
                           DATA_LENGTH  as dataLength,
                           NULLABLE     as nullable,
                           DATA_DEFAULT as dataDefault,
                           COMMENTS     as comments
                    FROM USER_TAB_COLUMNS
                    WHERE TABLE_NAME = UPPER('${table_name}')
                    ORDER BY COLUMN_ID
				`;
				const columnResult = await executeQuery(columnSql);

				// 查询主键
				const pkSql = `
                    SELECT cols.COLUMN_NAME
                    FROM USER_CONSTRAINTS cons
                             JOIN USER_CONS_COLUMNS cols ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
                    WHERE cons.TABLE_NAME = UPPER('${table_name}')
                      AND cons.CONSTRAINT_TYPE = 'P'
				`;
				const pkResult = await executeQuery(pkSql);
				const primaryKeys = (
						pkResult.rows || []
				).map(row => row[0] || row.COLUMN_NAME);

				const columns = (
						columnResult.rows || []
				).map(row => (
						{
							name:row[0] || row.COLUMNNAME,
							type:row[1] || row.DATATYPE,
							length:row[2] || row.DATALENGTH,
							nullable:(
									row[3] || row.NULLABLE
							) === 'Y',
							defaultValue:row[4] || row.DATADEFAULT,
							comments:row[5] || row.COMMENTS || '',
							isPrimaryKey:primaryKeys.includes(row[0] || row.COLUMNNAME),
						}
				));

				return {
					content:[
						{
							type:'text',
							text:JSON.stringify({
								success:true,
								tableName:table_name,
								columns,
								primaryKeys,
							}, null, 2),
						},
					],
				};
			}catch(err){
				return {
					content:[{type:'text', text:`错误：${err.message}`}],
					isError:true,
				};
			}
		},
);

// ==========================================
// 工具：查看表数据样本
// ==========================================
server.tool(
		'dm_sample_data',
		'查看指定表的前N行数据',
		{
			table_name:z.string().describe('表名称'),
			limit:z.number().optional().default(10).describe('返回行数，默认10'),
		},
		async({table_name, limit = 10}) => {
			try{
				const sql = `SELECT *
                             FROM ${table_name} LIMIT ${limit}`;
				const result = await executeQuery(sql);
				const rows = result.rows || [];
				const fields = result.metaData || [];

				const formattedRows = rows.map(row => {
					const obj = {};
					if(Array.isArray(row)){
						fields.forEach((field, index) => {
							obj[field.name || field.NAME || `col_${index}`] = row[index];
						});
					}else{
						Object.assign(obj, row);
					}
					return obj;
				});

				return {
					content:[
						{
							type:'text',
							text:JSON.stringify({
								success:true,
								tableName:table_name,
								rowCount:formattedRows.length,
								rows:formattedRows,
							}, null, 2),
						},
					],
				};
			}catch(err){
				return {
					content:[{type:'text', text:`错误：${err.message}`}],
					isError:true,
				};
			}
		},
);

// 启动服务器
async function main(){
	const transport = new StdioServerTransport();
	await server.connect(transport);
	console.error('✅ 达梦 DM8 MCP 服务启动成功！');
	console.error(`   连接信息: ${DM_CONFIG.user}@${DM_CONFIG.host}:${DM_CONFIG.port}`);
}

main().catch((error) => {
	console.error('❌ 启动失败:', error);
	process.exit(1);
});
import 'dotenv/config';
import {McpServer} from '@modelcontextprotocol/sdk/server/mcp.js';
import {StdioServerTransport} from '@modelcontextprotocol/sdk/server/stdio.js';
import {z} from 'zod';
import dmdb from 'dmdb';

// 达梦数据库配置
const DM_HOST = process.env.DM_HOST || 'localhost';
const DM_PORT = process.env.DM_PORT || '5237';
const DM_USER = process.env.DM_USER || 'SYSDBA';
const DM_PASSWORD = process.env.DM_PASSWORD || '';
const DM_SCHEMA = process.env.DM_SCHEMA || process.env.DM_USER || 'SYSDBA';

// 配置 dmdb 驱动：将 CLOB/TEXT/BUFFER 类型自动转为字符串
dmdb.fetchAsString = [dmdb.CLOB, dmdb.BUFFER];

// dmdb 驱动需要使用 connectString 格式
const DM_CONFIG = {
	connectString:`dm://${DM_USER}:${DM_PASSWORD}@${DM_HOST}:${DM_PORT}?schema=${DM_SCHEMA}`,
	poolAlias:'dameng-mcp',
	poolMax:10,
	poolMin:1,
	poolTimeout:60,
};

// 安全的 JSON 序列化（处理 BigInt、循环引用、特殊类型）
function safeJsonStringify(obj){
	const seen = new WeakSet();
	return JSON.stringify(obj, (key, value) => {
		// 处理 BigInt
		if(typeof value === 'bigint'){
			return value.toString();
		}
		// 处理 Buffer
		if(Buffer.isBuffer(value)){
			return value.toString('base64');
		}
		// 处理 Date
		if(value instanceof Date){
			return value.toISOString();
		}
		// 处理循环引用
		if(typeof value === 'object' && value !== null){
			if(seen.has(value)){
				return '[Circular]';
			}
			seen.add(value);
		}
		// 处理其他无法序列化的类型
		if(typeof value === 'function' || typeof value === 'symbol'){
			return undefined;
		}
		return value;
	}, 2);
}

// 验证表名（防止 SQL 注入）
function validateTableName(tableName){
	if(!tableName || typeof tableName !== 'string'){
		throw new Error('表名不能为空');
	}
	// 只允许字母、数字、下划线
	if(!/^[a-zA-Z_][a-zA-Z0-9_-]*$/.test(tableName)){
		throw new Error(`无效的表名: ${tableName}`);
	}
	return tableName.toUpperCase();
}

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
		'执行只读SQL查询(达梦数据库)',
		{
			sql:z.string().describe('SQL查询语句(仅支持 SELECT、SHOW、DESC、EXPLAIN)'),
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
				const rows = result.rows || [];
				const fields = result.metaData || [];

				// 转换为更友好的格式
				const formattedRows = rows.map(row => {
					const obj = {};
					if(Array.isArray(row)){
						fields.forEach((field, index) => {
							obj[field.name || `col_${index}`] = row[index];
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
							text:safeJsonStringify({
								success:true,
								rowCount:formattedRows.length,
								fields:fields.map(f => f.name),
								rows:formattedRows,
							}),
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
							text:safeJsonStringify({
								success:true,
								tables:rows.map(row => (
										{
											name:row[0] || row.TABLENAME,
											comments:row[1] || row.COMMENTS || '',
										}
								)),
							}),
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
				// 验证表名
				const validatedTableName = validateTableName(table_name);

				// 查询列信息
				const columnSql = `
                    SELECT COLUMN_NAME  as columnName,
                           DATA_TYPE    as dataType,
                           DATA_LENGTH  as dataLength,
                           NULLABLE     as nullable,
                           DATA_DEFAULT as dataDefault
                    FROM USER_TAB_COLUMNS
                    WHERE TABLE_NAME = '${validatedTableName}'
                    ORDER BY COLUMN_ID
				`;
				const columnResult = await executeQuery(columnSql);

				if(!columnResult.rows || columnResult.rows.length === 0){
					return {
						content:[{type:'text', text:`错误：表 ${table_name} 不存在`}],
						isError:true,
					};
				}

				// 查询列注释
				const commentSql = `
                    SELECT COLUMN_NAME, COMMENTS
                    FROM USER_COL_COMMENTS
                    WHERE TABLE_NAME = '${validatedTableName}'
				`;
				const commentResult = await executeQuery(commentSql);
				const commentMap = {};
				(
						commentResult.rows || []
				).forEach(row => {
					commentMap[row[0] || row.COLUMN_NAME] = row[1] || row.COMMENTS || '';
				});

				// 查询主键
				const pkSql = `
                    SELECT cols.COLUMN_NAME
                    FROM USER_CONSTRAINTS cons
                             JOIN USER_CONS_COLUMNS cols ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
                    WHERE cons.TABLE_NAME = '${validatedTableName}'
                      AND cons.CONSTRAINT_TYPE = 'P'
				`;
				const pkResult = await executeQuery(pkSql);
				const primaryKeys = (
						pkResult.rows || []
				).map(row => row[0] || row.COLUMN_NAME);

				const columns = (
						columnResult.rows || []
				).map(row => {
					const colName = row[0] || row.COLUMNNAME;
					return {
						name:colName,
						type:row[1] || row.DATATYPE,
						length:row[2] || row.DATALENGTH,
						nullable:(
								row[3] || row.NULLABLE
						) === 'Y',
						defaultValue:row[4] || row.DATADEFAULT,
						comments:commentMap[colName] || '',
						isPrimaryKey:primaryKeys.includes(colName),
					};
				});

				return {
					content:[
						{
							type:'text',
							text:safeJsonStringify({
								success:true,
								tableName:table_name,
								columns,
								primaryKeys,
							}),
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
				// 验证表名
				const validatedTableName = validateTableName(table_name);

				// 限制最大行数
				const safeLimit = Math.min(Math.max(1, limit), 1000);

				const sql = `SELECT *
                             FROM ${validatedTableName} LIMIT ${safeLimit}`;
				const result = await executeQuery(sql);
				const rows = result.rows || [];
				const fields = result.metaData || [];

				const formattedRows = rows.map(row => {
					const obj = {};
					if(Array.isArray(row)){
						fields.forEach((field, index) => {
							obj[field.name || `col_${index}`] = row[index];
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
							text:safeJsonStringify({
								success:true,
								tableName:table_name,
								rowCount:formattedRows.length,
								rows:formattedRows,
							}),
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
	console.error(`   连接信息: ${DM_USER}@${DM_HOST}:${DM_PORT}`);
	console.error(`   配置: fetchAsString=[CLOB, BUFFER]`);
}

main().catch((error) => {
	console.error('❌ 启动失败:', error);
	process.exit(1);
});
#!/usr/bin/env node
import 'dotenv/config';
import {McpServer} from '@modelcontextprotocol/sdk/server/mcp.js';
import {StdioServerTransport} from '@modelcontextprotocol/sdk/server/stdio.js';
import {z} from 'zod';
import dmdb from 'dmdb';

//region 常量定义
const MAX_ROWS = 1000; // dm_sample_data 最大返回行数
const READ_ONLY_SQL_PATTERN = /^(SELECT|SHOW|DESC|DESCRIBE|EXPLAIN|WITH)(\s|\(|\*)/i; // 只读SQL正则
const TABLE_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_-]*$/; // 表名验证正则（防SQL注入）
//endregion
//region 达梦数据库配置
const DM_HOST = process.env.DM_HOST || 'localhost';
const DM_PORT = process.env.DM_PORT || '5237';
const DM_USER = process.env.DM_USER || 'SYSDBA';
const DM_PASSWORD = process.env.DM_PASSWORD || '';
const DM_SCHEMA = process.env.DM_SCHEMA || process.env.DM_USER || 'SYSDBA';
// 配置驱动：自动将 CLOB/BUFFER 类型转为字符串
dmdb.fetchAsString = [dmdb.CLOB, dmdb.BUFFER];
const DM_CONFIG = {
	connectString:`dm://${DM_USER}:${DM_PASSWORD}@${DM_HOST}:${DM_PORT}?schema=${DM_SCHEMA}`,
	poolAlias:'dameng-mcp',
	poolMax:10,
	poolMin:1,
	poolTimeout:60,
	poolIncrement:1,
	poolPingInterval:30,
	connectionTimeout:10,
	queueTimeout:30,
};
//endregion
//region 工具函数
//安全的 JSON 序列化（处理 BigInt、Buffer、Date、循环引用）
function safeJsonStringify(obj){
	const seen = [];
	return JSON.stringify(obj, (key, value) => {
		if(typeof value === 'bigint'){
			return value.toString();
		}
		if(Buffer.isBuffer(value)){
			return value.toString('base64');
		}
		if(value instanceof Date){
			return value.toISOString();
		}
		if(typeof value === 'object' && value !== null){
			for(let i = 0; i < seen.length; i++){
				if(seen[i] === value){
					return '[Circular]';
				}
			}
			seen.push(value);
		}
		if(typeof value === 'function' || typeof value === 'symbol'){
			return undefined;
		}
		return value;
	});
}
//验证表名（防止 SQL 注入）
function validateTableName(tableName){
	if(!tableName || typeof tableName !== 'string'){
		throw new Error('表名不能为空');
	}
	if(!TABLE_NAME_REGEX.test(tableName)){
		throw new Error(`无效的表名: ${tableName}`);
	}
	return tableName.toUpperCase();
}
//格式化查询结果行数据（使用 for 循环优化性能，兼容数组和对象格式）
function formatRows(rows, fields){
	if(!rows || rows.length === 0){
		return [];
	}
	const fieldNames = fields.map(f => f.name);
	const result = new Array(rows.length);
	const isArrayFormat = Array.isArray(rows[0]);
	if(isArrayFormat){
		for(let i = 0; i < rows.length; i++){
			const row = rows[i];
			const obj = {};
			for(let j = 0; j < fieldNames.length; j++){
				obj[fieldNames[j]] = row[j];
			}
			result[i] = obj;
		}
	}else{
		for(let i = 0; i < rows.length; i++){
			const row = rows[i];
			const obj = {};
			for(let j = 0; j < fieldNames.length; j++){
				obj[fieldNames[j]] = row[fieldNames[j]];
			}
			result[i] = obj;
		}
	}
	return result;
}
//获取行数据字段值（兼容数组格式和对象格式）
function getFieldValue(row, fieldIndex, fieldName){
	if(!row){
		return null;
	}
	return row[fieldIndex] ?? row[fieldName] ?? null;
}
//构建成功响应
function successResponse(data){
	return {content:[{type:'text', text:safeJsonStringify({success:true, ...data})}]};
}
//构建错误响应
function errorResponse(message){
	return {content:[{type:'text', text:`错误：${message}`}], isError:true};
}
//endregion
//region 数据库连接池
let poolPromise = null;
async function getPool(){
	if(!poolPromise){
		poolPromise = dmdb.createPool(DM_CONFIG).catch(err => {
			poolPromise = null; // 创建失败时重置，允许重试
			throw err;
		});
	}
	return poolPromise;
}
async function executeQuery(sql){
	const pool = await getPool();
	const conn = await pool.getConnection();
	try{
		return await conn.execute(sql);
	}finally{
		await conn.close(); // 确保连接释放
	}
}
async function closePool(){
	if(poolPromise){
		try{
			const pool = await poolPromise;
			await pool.close();
		}catch(err){
			console.error('关闭连接池失败:', err.message);
		}finally{
			poolPromise = null;
		}
	}
}
//endregion
//region MCP 服务器
const server = new McpServer(
	{name:'dameng-mcp', version:'1.0.0'},
	{capabilities:{tools:{}}},
);
//endregion
//region 工具：执行 SQL 查询
server.tool(
	'dm_query',
	'执行只读SQL查询，返回行数、字段名列表和数据行。支持SELECT/SHOW/DESC/DESCRIBE/EXPLAIN/WITH语句。',
	{sql:z.string().describe('SQL查询语句(仅支持SELECT/SHOW/DESC/DESCRIBE/EXPLAIN/WITH)')},
	async({sql}) => {
		try{
			if(!READ_ONLY_SQL_PATTERN.test(sql.trim())){
				return errorResponse('仅允许执行只读SQL（SELECT、SHOW、DESC、EXPLAIN）');
			}
			const result = await executeQuery(sql);
			const rows = result.rows || [];
			const fields = result.metaData || [];
			return successResponse({
				rowCount:rows.length,
				fields:fields.map(f => f.name),
				rows:formatRows(rows, fields),
			});
		}catch(err){
			return errorResponse(err.message);
		}
	},
);
//endregion
//region 工具：列出所有表
server.tool(
	'dm_list_tables',
	'列出当前用户有权限访问的所有表，返回表总数和表列表（表名、表注释），按表名排序。',
	{},
	async() => {
		try{
			const result = await executeQuery(`
                SELECT TABLE_NAME as tableName, COMMENTS as comments
                FROM USER_TAB_COMMENTS
                WHERE TABLE_TYPE = 'TABLE'
                ORDER BY TABLE_NAME
			`);
			const rows = result.rows || [];
			const tables = new Array(rows.length);
			for(let i = 0; i < rows.length; i++){
				tables[i] = {
					name:getFieldValue(rows[i], 0, 'TABLENAME'),
					comments:getFieldValue(rows[i], 1, 'COMMENTS') || '',
				};
			}
			return successResponse({totalCount:tables.length, tables});
		}catch(err){
			return errorResponse(err.message);
		}
	},
);
//endregion
//region 工具：查看表结构
server.tool(
	'dm_describe_table',
	'查看指定表的完整结构信息，返回：表名、表注释、列信息(名称/类型/长度/精度/标度/nullable/默认值/注释/是否主键)、主键列表、索引列表(名称/是否唯一/包含列)。',
	{table_name:z.string().describe('要查看的表名称(只支持字母/数字/下划线/横线组成的合法表名)')},
	async({table_name}) => {
		try{
			const validatedTableName = validateTableName(table_name);
			// 并行执行5个查询（性能优化）
			const [columnResult, tableCommentResult, commentResult, pkResult, idxResult] = await Promise.all(
				[
					executeQuery(`
                        SELECT COLUMN_NAME    as columnName,
                               DATA_TYPE      as dataType,
                               DATA_LENGTH    as dataLength,
                               DATA_PRECISION as dataPrecision,
                               DATA_SCALE     as dataScale,
                               NULLABLE       as nullable,
                               DATA_DEFAULT   as dataDefault
                        FROM USER_TAB_COLUMNS
                        WHERE TABLE_NAME = '${validatedTableName}'
                        ORDER BY COLUMN_ID
					`),
					executeQuery(`
                        SELECT COMMENTS
                        FROM USER_TAB_COMMENTS
                        WHERE TABLE_NAME = '${validatedTableName}'
                          AND TABLE_TYPE = 'TABLE'
					`),
					executeQuery(`
                        SELECT COLUMN_NAME, COMMENTS
                        FROM USER_COL_COMMENTS
                        WHERE TABLE_NAME = '${validatedTableName}'
					`),
					executeQuery(`
                        SELECT cols.COLUMN_NAME
                        FROM USER_CONSTRAINTS cons
                                 JOIN USER_CONS_COLUMNS cols ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
                        WHERE cons.TABLE_NAME = '${validatedTableName}'
                          AND cons.CONSTRAINT_TYPE = 'P'
					`),
					executeQuery(`
                        SELECT i.INDEX_NAME, i.UNIQUENESS, ic.COLUMN_NAME, ic.COLUMN_POSITION
                        FROM USER_INDEXES i
                                 JOIN USER_IND_COLUMNS ic ON i.INDEX_NAME = ic.INDEX_NAME
                        WHERE i.TABLE_NAME = '${validatedTableName}'
                          AND NOT EXISTS (SELECT 1
                                          FROM USER_CONSTRAINTS c
                                          WHERE c.CONSTRAINT_NAME = i.INDEX_NAME
                                            AND c.CONSTRAINT_TYPE = 'P')
                        ORDER BY i.INDEX_NAME, ic.COLUMN_POSITION
					`),
				]);
			if(!columnResult.rows || columnResult.rows.length === 0){
				return errorResponse(`表 ${table_name} 不存在`);
			}
			const tableComments = getFieldValue(tableCommentResult.rows?.[0] || [], 0, 'COMMENTS')
				|| '';
			// 使用 Map/Set 优化查找性能（O(1)复杂度）
			const commentMap = new Map((
				commentResult.rows || []
			).map(row => [
				getFieldValue(row, 0, 'COLUMN_NAME'),
				getFieldValue(row, 1, 'COMMENTS') || '',
			]));
			const primaryKeySet = new Set((
				pkResult.rows || []
			).map(row =>
				getFieldValue(row, 0, 'COLUMN_NAME'),
			));
			// 构建索引信息
			const indexMap = new Map();
			(
				idxResult.rows || []
			).forEach(row => {
				const idxName = getFieldValue(row, 0, 'INDEX_NAME');
				if(!indexMap.has(idxName)){
					indexMap.set(idxName, {
						name:idxName,
						unique:getFieldValue(row, 1, 'UNIQUENESS') === 'UNIQUE',
						columns:[],
					});
				}
				indexMap.get(idxName).columns.push(getFieldValue(row, 2, 'COLUMN_NAME'));
			});
			// 构建列信息
			const columns = (
				columnResult.rows || []
			).map(row => {
				const colName = getFieldValue(row, 0, 'COLUMNNAME');
				return {
					name:colName,
					type:getFieldValue(row, 1, 'DATATYPE'),
					length:getFieldValue(row, 2, 'DATALENGTH'),
					precision:getFieldValue(row, 3, 'DATAPRECISION'),
					scale:getFieldValue(row, 4, 'DATASCALE'),
					nullable:getFieldValue(row, 5, 'NULLABLE') === 'Y',
					defaultValue:getFieldValue(row, 6, 'DATADEFAULT'),
					comments:commentMap.get(colName) || '',
					isPrimaryKey:primaryKeySet.has(colName),
				};
			});
			return successResponse({
				tableName:table_name,
				tableComments,
				columns,
				primaryKeys:Array.from(primaryKeySet),
				indexes:Array.from(indexMap.values()),
			});
		}catch(err){
			return errorResponse(err.message);
		}
	},
);
//endregion
//region 工具：查看表数据样本
server.tool(
	'dm_sample_data',
	'查看指定表的前N行数据样本，返回表名、行数、字段名列表和数据行。limit参数自动修正：小于1时修正为1，大于1000时限制为1000。',
	{
		table_name:z.string().describe('要查看的表名称(只支持字母/数字/下划线/横线组成的合法表名)'),
		limit:z.number()
			.optional()
			.default(10)
			.describe('返回行数，范围1-1000，默认10，超出范围自动修正'),
	},
	async({table_name, limit = 10}) => {
		try{
			const validatedTableName = validateTableName(table_name);
			const safeLimit = Math.min(Math.max(1, limit), MAX_ROWS);
			const result = await executeQuery(`SELECT *
                                               FROM ${validatedTableName} LIMIT ${safeLimit}`);
			const rows = result.rows || [];
			const fields = result.metaData || [];
			return successResponse({
				tableName:table_name,
				rowCount:rows.length,
				fields:fields.map(f => f.name),
				rows:formatRows(rows, fields),
			});
		}catch(err){
			return errorResponse(err.message);
		}
	},
);
//endregion
//region 启动服务器
async function main(){
	const transport = new StdioServerTransport();
	await server.connect(transport);
}
main().catch((error) => {
	console.error('❌ 启动失败:', error);
	process.exit(1);
});
// 进程退出时关闭连接池
process.on('SIGINT', async() => {
	await closePool();
	process.exit(0);
});
process.on('SIGTERM', async() => {
	await closePool();
	process.exit(0);
});
//endregion
#!/usr/bin/env node
import 'dotenv/config';
import {
	McpServer,
	ResourceTemplate,
} from '@modelcontextprotocol/sdk/server/mcp.js';
import {StdioServerTransport} from '@modelcontextprotocol/sdk/server/stdio.js';
import {z} from 'zod';
import dmdb from 'dmdb';

//region 常量定义
const MAX_ROWS = 1000;
const READ_ONLY_SQL_PATTERN = /^(SELECT|SHOW|DESC|DESCRIBE|EXPLAIN|WITH)(\s|\(|\*)/i;
const TABLE_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_-]*$/;
const DANGEROUS_SQL_KEYWORDS = /\b(DELETE|INSERT|UPDATE|ALTER|DROP|CREATE|TRUNCATE|GRANT|REVOKE|COMMIT|ROLLBACK)\b/i;
//endregion
//region 达梦数据库配置
const DM_HOST = process.env.DM_HOST || 'localhost';
const DM_PORT = process.env.DM_PORT || '5237';
const DM_USER = process.env.DM_USER || 'SYSDBA';
const DM_PASSWORD = process.env.DM_PASSWORD || '';
const DM_SCHEMA = process.env.DM_SCHEMA || process.env.DM_USER || 'SYSDBA';

if(!DM_PASSWORD){
	console.error('❌ 错误: 请设置 DM_PASSWORD 环境变量或 .env 文件');
	process.exit(1);
}

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

function validateTableName(tableName){
	if(!tableName || typeof tableName !== 'string'){
		throw new Error('表名不能为空');
	}
	if(!TABLE_NAME_REGEX.test(tableName)){
		throw new Error(`无效的表名: ${tableName}`);
	}
	return tableName.toUpperCase();
}

function validateReadOnlySql(sql){
	const trimmed = sql.trim();
	if(!READ_ONLY_SQL_PATTERN.test(trimmed)){
		return {valid:false, reason:'仅允许执行只读SQL（SELECT、SHOW、DESC、EXPLAIN、WITH）'};
	}
	if(trimmed.includes(';')){
		return {valid:false, reason:'SQL语句不能包含分号（防止多语句执行）'};
	}
	if(DANGEROUS_SQL_KEYWORDS.test(trimmed)){
		return {valid:false, reason:'SQL语句包含危险关键字（DELETE/INSERT/UPDATE/ALTER/DROP等）'};
	}
	return {valid:true};
}

function formatRows(rows, fields){
	if(!rows || rows.length === 0){
		return [];
	}
	const fieldNames = fields.map(f => f.name);
	const result = new Array(rows.length);
	const isArrayFormat = Array.isArray(rows[0]);
	for(let i = 0; i < rows.length; i++){
		const row = rows[i];
		const obj = {};
		for(let j = 0; j < fieldNames.length; j++){
			obj[fieldNames[j]] = isArrayFormat
				? row[j]
				: row[fieldNames[j]];
		}
		result[i] = obj;
	}
	return result;
}

function getFieldValue(row, fieldIndex, fieldName){
	if(!row){
		return null;
	}
	return row[fieldIndex] ?? row[fieldName] ?? null;
}

function buildTableStructure(columnRows, tableCommentRows, commentRows, pkRows, idxRows, fkRows){
	const tableComments = getFieldValue(tableCommentRows?.[0] || [], 0, 'COMMENTS') || '';
	const commentMap = new Map((
		commentRows || []
	).map(row => [
		getFieldValue(row, 0, 'COLUMN_NAME'),
		getFieldValue(row, 1, 'COMMENTS') || '',
	]));
	// 使用 Set 优化主键查找性能 O(1)
	const primaryKeySet = new Set((
		pkRows || []
	).map(row => getFieldValue(row, 0, 'COLUMN_NAME')));

	const indexMap = new Map();
	(
		idxRows || []
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

	const fkMap = new Map();
	(
		fkRows || []
	).forEach(row => {
		const fkName = getFieldValue(row, 0, 'CONSTRAINTNAME');
		if(!fkMap.has(fkName)){
			fkMap.set(fkName, {
				name:fkName,
				columns:[],
				refTable:getFieldValue(row, 3, 'REFTABLENAME'),
				refColumns:[],
			});
		}
		const fk = fkMap.get(fkName);
		fk.columns.push(getFieldValue(row, 1, 'COLUMNNAME'));
		fk.refColumns.push(getFieldValue(row, 4, 'REFCOLUMNNAME'));
	});

	const columns = (
		columnRows || []
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

	return {
		columns,
		tableComments,
		primaryKeys:Array.from(primaryKeySet),
		indexes:Array.from(indexMap.values()),
		foreignKeys:Array.from(fkMap.values()),
	};
}
//endregion
//region SQL 查询集中管理
const QUERIES = {
	listTables:(includeViews) => `
        SELECT TABLE_NAME as tableName, COMMENTS as comments, TABLE_TYPE as tableType
        FROM USER_TAB_COMMENTS
        WHERE TABLE_TYPE ${includeViews
                ? 'IN (\'TABLE\', \'VIEW\')'
                : '= \'TABLE\''}
        ORDER BY TABLE_TYPE, TABLE_NAME
	`,
	describeTable:(name) => [
		`SELECT COLUMN_NAME    as columnName,
                DATA_TYPE      as dataType,
                DATA_LENGTH    as dataLength,
                DATA_PRECISION as dataPrecision,
                DATA_SCALE     as dataScale,
                NULLABLE       as nullable,
                DATA_DEFAULT   as dataDefault
         FROM USER_TAB_COLUMNS
         WHERE TABLE_NAME = '${name}'
         ORDER BY COLUMN_ID`,
		`SELECT COMMENTS
         FROM USER_TAB_COMMENTS
         WHERE TABLE_NAME = '${name}'
           AND TABLE_TYPE = 'TABLE'`,
		`SELECT COLUMN_NAME, COMMENTS
         FROM USER_COL_COMMENTS
         WHERE TABLE_NAME = '${name}'`,
		`SELECT cols.COLUMN_NAME
         FROM USER_CONSTRAINTS cons
                  JOIN USER_CONS_COLUMNS cols ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
         WHERE cons.TABLE_NAME = '${name}'
           AND cons.CONSTRAINT_TYPE = 'P'`,
		`SELECT i.INDEX_NAME, i.UNIQUENESS, ic.COLUMN_NAME, ic.COLUMN_POSITION
         FROM USER_INDEXES i
                  JOIN USER_IND_COLUMNS ic ON i.INDEX_NAME = ic.INDEX_NAME
         WHERE i.TABLE_NAME = '${name}'
           AND NOT EXISTS (SELECT 1
                           FROM USER_CONSTRAINTS c
                           WHERE c.CONSTRAINT_NAME = i.INDEX_NAME
                             AND c.CONSTRAINT_TYPE = 'P')
         ORDER BY i.INDEX_NAME, ic.COLUMN_POSITION`,
		`SELECT cons.CONSTRAINT_NAME as constraintName,
                cols.COLUMN_NAME     as columnName,
                cols.POSITION as position, r_cons.TABLE_NAME as refTableName,
			   r_cols.COLUMN_NAME as refColumnName
         FROM USER_CONSTRAINTS cons
             JOIN USER_CONS_COLUMNS cols
         ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
             JOIN USER_CONSTRAINTS r_cons ON cons.R_CONSTRAINT_NAME = r_cons.CONSTRAINT_NAME
             JOIN USER_CONS_COLUMNS r_cols ON r_cons.CONSTRAINT_NAME = r_cols.CONSTRAINT_NAME
             AND cols.POSITION = r_cols.POSITION
         WHERE cons.TABLE_NAME = '${name}' AND cons.CONSTRAINT_TYPE = 'R'
         ORDER BY cons.CONSTRAINT_NAME, cols.POSITION`,
	],
	sampleData:(name, limit) => `SELECT *
                                 FROM ${name} LIMIT ${limit}`,
};

// 高复用：获取表结构原始数据
async function fetchTableMetadata(tableName){
	const validatedName = validateTableName(tableName);
	const queries = QUERIES.describeTable(validatedName);
	const results = await Promise.all(queries.map(executeQuery));
	return results;
}
//endregion
//region 数据库连接池
let poolPromise = null;
async function getPool(){
	if(!poolPromise){
		poolPromise = dmdb.createPool(DM_CONFIG).catch(err => {
			poolPromise = null;
			throw err;
		});
	}
	return poolPromise;
}

// 支持 maxRows 参数
async function executeQuery(sql, options = {}){
	const pool = await getPool();
	const conn = await pool.getConnection();
	try{
		return await conn.execute(sql, options);
	}finally{
		try{
			await conn.close();
		}catch(e){
			// 忽略关闭异常，防止掩盖原始错误
		}
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
	{name:'dameng-mcp', version:'1.1.0'},
	{capabilities:{tools:{}, resources:{}}},
);
//endregion
//region 工具：执行 SQL 查询
server.tool(
	'dm_query',
	'执行只读SQL查询，返回行数、字段名列表和数据行。支持SELECT/SHOW/DESC/DESCRIBE/EXPLAIN/WITH语句。',
	{
		sql:z.string().describe('SQL查询语句(仅支持SELECT/SHOW/DESC/DESCRIBE/EXPLAIN/WITH)'),
		max_rows:z.number()
			.optional()
			.default(MAX_ROWS)
			.describe('最大返回行数，默认1000，超出自动截断'),
	},
	async({sql, max_rows}) => {
		try{
			const validation = validateReadOnlySql(sql);
			if(!validation.valid){
				return errorResponse(validation.reason);
			}
			const result = await executeQuery(sql);
			const rows = result.rows || [];
			const fields = result.metaData || [];
			// 采用 JS 层截断，确保响应体积可控，兼容所有驱动实现
			const isTruncated = rows.length > max_rows;
			const limitedRows = isTruncated
				? rows.slice(0, max_rows)
				: rows;
			return successResponse({
				rowCount:limitedRows.length,
				totalRowCount:rows.length,
				isTruncated,
				fields:fields.map(f => f.name),
				rows:formatRows(limitedRows, fields),
			});
		}catch(err){
			return errorResponse(err.message || String(err));
		}
	},
);
//endregion
//region 工具：列出所有表
server.tool(
	'dm_list_tables',
	'列出当前用户有权限访问的所有表（和视图），返回总数和列表（表名、类型、注释），按名称排序。',
	{
		include_views:z.boolean().optional().default(false).describe('是否包含视图，默认false'),
	},
	async({include_views}) => {
		try{
			const result = await executeQuery(QUERIES.listTables(include_views));
			const tables = (
				result.rows || []
			).map(row => (
				{
					name:getFieldValue(row, 0, 'TABLENAME'),
					type:getFieldValue(row, 2, 'TABLETYPE') || 'TABLE',
					comments:getFieldValue(row, 1, 'COMMENTS') || '',
				}
			));
			return successResponse({totalCount:tables.length, tables});
		}catch(err){
			return errorResponse(err.message || String(err));
		}
	},
);
//endregion
//region 工具：查看表结构
server.tool(
	'dm_describe_table',
	'查看指定表的完整结构信息，返回：表名、表注释、列信息、主键列表、索引列表、外键列表。',
	{table_name:z.string().describe('要查看的表名称(只支持字母/数字/下划线/横线组成的合法表名)')},
	async({table_name}) => {
		try{
			const results = await fetchTableMetadata(table_name);
			if(!results[0].rows || results[0].rows.length === 0){
				return errorResponse(`表 ${table_name} 不存在`);
			}
			const structure = buildTableStructure(...results.map(r => r.rows));
			return successResponse({tableName:table_name, ...structure});
		}catch(err){
			return errorResponse(err.message || String(err));
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
			const result = await executeQuery(QUERIES.sampleData(validatedTableName, safeLimit));
			const rows = result.rows || [];
			const fields = result.metaData || [];
			return successResponse({
				tableName:table_name,
				rowCount:rows.length,
				fields:fields.map(f => f.name),
				rows:formatRows(rows, fields),
			});
		}catch(err){
			return errorResponse(err.message || String(err));
		}
	},
);
//endregion
//region MCP Resource：数据库表列表
server.resource(
	'数据库表列表',
	'schema://tables',
	{
		description:'列出当前用户有权限访问的所有表和视图，包含表名、类型和注释。',
		mimeType:'application/json',
	},
	async(uri) => {
		try{
			const result = await executeQuery(QUERIES.listTables(true));
			const tables = (
				result.rows || []
			).map(row => (
				{
					name:getFieldValue(row, 0, 'TABLENAME'),
					type:getFieldValue(row, 2, 'TABLETYPE') || 'TABLE',
					comments:getFieldValue(row, 1, 'COMMENTS') || '',
				}
			));
			return {
				contents:[
					{
						uri:uri.href, mimeType:'application/json',
						text:safeJsonStringify({totalCount:tables.length, tables}),
					},
				],
			};
		}catch(err){
			return {contents:[{uri:uri.href, text:`错误：${err.message || String(err)}`}]};
		}
	},
);
//endregion
//region MCP Resource：单个表结构
server.resource(
	'表结构信息',
	new ResourceTemplate('table://{name}', {list:undefined}),
	{
		description:'查看指定表的完整结构（列、主键、索引、外键）。例如：table://USER_INFO',
		mimeType:'application/json',
	},
	async(uri, variables) => {
		try{
			const tableName = variables.name;
			const results = await fetchTableMetadata(tableName);
			if(!results[0].rows || results[0].rows.length === 0){
				return {contents:[{uri:uri.href, text:`表 ${tableName} 不存在`}]};
			}
			const structure = buildTableStructure(...results.map(r => r.rows));
			return {
				contents:[
					{
						uri:uri.href, mimeType:'application/json',
						text:safeJsonStringify({tableName, ...structure}),
					},
				],
			};
		}catch(err){
			return {contents:[{uri:uri.href, text:`错误：${err.message || String(err)}`}]};
		}
	},
);
//endregion
//region 响应构建
function successResponse(data){
	return {content:[{type:'text', text:safeJsonStringify({success:true, ...data})}]};
}
function errorResponse(message){
	return {
		content:[{type:'text', text:safeJsonStringify({success:false, error:message})}],
		isError:true,
	};
}
//endregion
//region 启动服务器
async function main(){
	const transport = new StdioServerTransport();
	await server.connect(transport);
	console.error('✅ 达梦 DM8 MCP 服务启动成功');
	console.error(`   连接: ${DM_USER}@${DM_HOST}:${DM_PORT}/${DM_SCHEMA}`);
}
main().catch((error) => {
	console.error('❌ 启动失败:', error);
	process.exit(1);
});

process.on('SIGINT', () => {
	closePool().finally(() => process.exit(0));
});
process.on('SIGTERM', () => {
	closePool().finally(() => process.exit(0));
});
//endregion
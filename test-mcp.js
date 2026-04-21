#!/usr/bin/env node
/**
 * 达梦 MCP Server 全面测试脚本
 * 模拟 MCP 客户端通过 stdio 与服务器通信，测试所有工具和场景
 */
import {spawn} from 'child_process';
import {once} from 'events';
import {readFileSync} from 'fs';

// 测试配置
const SERVER_CMD = 'node';
const SERVER_ARGS = ['mcp-dameng.js'];
const TEST_TIMEOUT = 30000;

// 测试统计
const results = {passed: 0, failed: 0, skipped: 0, tests: []};

// 颜色输出
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const CYAN = '\x1b[36m';
const BOLD = '\x1b[1m';
const RESET = '\x1b[0m';

class McpTestClient {
	constructor() {
		this.server = null;
		this.requestId = 0;
		this.pendingRequests = new Map();
		this.outputBuffer = '';
	}

	async start() {
		this.server = spawn(SERVER_CMD, SERVER_ARGS, {
			env: {...process.env},
			stdio: ['pipe', 'pipe', 'pipe'],
		});

		this.server.stdout.on('data', (data) => {
			this.outputBuffer += data.toString();
			this.processOutput();
		});

		this.server.stderr.on('data', (data) => {
			console.error(YELLOW + '[Server Log] ' + RESET + data.toString().trim());
		});

		this.server.on('error', (err) => {
			console.error(RED + '[Server Error] ' + RESET + err.message);
		});

		// 等待服务器就绪
		await new Promise((resolve, reject) => {
			const timer = setTimeout(() => reject(new Error('启动超时')), 10000);
			this.server.stderr.on('data', (data) => {
				if (data.toString().includes('启动成功')) {
					clearTimeout(timer);
					resolve();
				}
			});
		});
	}

	processOutput() {
		const lines = this.outputBuffer.split('\n');
		this.outputBuffer = lines.pop() || '';

		for (const line of lines) {
			if (line.trim()) {
				try {
					const msg = JSON.parse(line);
					if (msg.id !== undefined && this.pendingRequests.has(msg.id)) {
						const {resolve, reject} = this.pendingRequests.get(msg.id);
						this.pendingRequests.delete(msg.id);
						if (msg.error) reject(new Error(msg.error.message || JSON.stringify(msg.error)));
						else resolve(msg.result);
					}
				} catch (e) {
					// 忽略非 JSON 行
				}
			}
		}
	}

	async sendRequest(method, params = {}) {
		const id = ++this.requestId;
		const request = {jsonrpc: '2.0', id, method, params};
		this.server.stdin.write(JSON.stringify(request) + '\n');

		return new Promise((resolve, reject) => {
			this.pendingRequests.set(id, {resolve, reject});
			setTimeout(() => {
				if (this.pendingRequests.has(id)) {
					this.pendingRequests.delete(id);
					reject(new Error('请求超时'));
				}
			}, TEST_TIMEOUT);
		});
	}

	async initialize() {
		return this.sendRequest('initialize', {
			protocolVersion: '2024-11-05',
			capabilities: {},
			clientInfo: {name: 'test-client', version: '1.0.0'},
		});
	}

	async callTool(name, args = {}) {
		return this.sendRequest('tools/call', {name, arguments: args});
	}

	async readResource(uri) {
		return this.sendRequest('resources/read', {uri});
	}

	stop() {
		if (this.server) {
			this.server.stdin.end();
			this.server.kill('SIGTERM');
		}
	}
}

// 断言工具
function assert(condition, message) {
	if (!condition) throw new Error(message);
}

function assertSuccess(result) {
	const content = JSON.parse(result.content[0].text);
	assert(content.success === true, `预期 success=true，实际: ${JSON.stringify(content)}`);
	return content;
}

function assertError(result, expectedMessage) {
	const content = result.content[0].text;
	assert(result.isError === true || content.startsWith('错误'), `预期错误响应，实际: ${content}`);
	if (expectedMessage) {
		assert(content.includes(expectedMessage), `预期错误包含 "${expectedMessage}"，实际: ${content}`);
	}
}

// 测试用例
async function runTests(client) {
	console.log('\n' + BOLD + '═══════════════════════════════════════════════' + RESET);
	console.log(BOLD + '🧪 达梦 MCP Server 全面测试' + RESET);
	console.log(BOLD + '═══════════════════════════════════════════════' + RESET + '\n');

	// ==================== 1. 初始化测试 ====================
	await testSection('1. MCP 协议初始化', async () => {
		const result = await client.initialize();
		assert(result.protocolVersion, '缺少 protocolVersion');
		assert(result.capabilities, '缺少 capabilities');
		assert(result.capabilities.tools, '缺少 tools 能力');
		assert(result.capabilities.resources, '缺少 resources 能力');
		console.log('   ✅ 协议版本: ' + result.protocolVersion);
		console.log('   ✅ 服务器: ' + result.serverInfo.name + ' v' + result.serverInfo.version);
	});

	// ==================== 2. dm_list_tables 测试 ====================
	await testSection('2. dm_list_tables - 列出所有表', async () => {
		const result = await client.callTool('dm_list_tables');
		const data = assertSuccess(result);
		assert(data.totalCount > 0, '表数量应大于 0');
		assert(Array.isArray(data.tables), 'tables 应为数组');
		assert(data.tables[0].name, '表应包含 name 字段');
		assert(data.tables[0].comments !== undefined, '表应包含 comments 字段');
		console.log(`   ✅ 找到 ${data.totalCount} 个表`);
		console.log(`   ✅ 示例表: ${data.tables.slice(0, 3).map(t => t.name).join(', ')}...`);
	});

	await testSection('2.1 dm_list_tables - 包含视图', async () => {
		const result = await client.callTool('dm_list_tables', {include_views: true});
		const data = assertSuccess(result);
		assert(data.totalCount > 0, '表+视图数量应大于 0');
		const hasView = data.tables.some(t => t.type === 'VIEW');
		console.log(`   ✅ 找到 ${data.totalCount} 个对象（表+视图）`);
		console.log(`   ${hasView ? '✅ 包含视图' : 'ℹ️  当前库无视图'}`);
	});

	// ==================== 3. dm_describe_table 测试 ====================
	await testSection('3. dm_describe_table - 查看表结构', async () => {
		// 先获取一个有效表名
		const listResult = assertSuccess(await client.callTool('dm_list_tables'));
		const tableName = listResult.tables[0].name;

		const result = await client.callTool('dm_describe_table', {table_name: tableName});
		const data = assertSuccess(result);
		assert(data.tableName === tableName, '表名应匹配');
		assert(Array.isArray(data.columns), '应有 columns 字段');
		assert(data.columns.length > 0, '表应有列');
		assert(data.columns[0].name, '列应有 name 字段');
		assert(data.columns[0].type, '列应有 type 字段');
		assert(Array.isArray(data.primaryKeys), '应有 primaryKeys 字段');
		assert(Array.isArray(data.indexes), '应有 indexes 字段');
		assert(Array.isArray(data.foreignKeys), '应有 foreignKeys 字段（新增功能）');

		console.log(`   ✅ 表: ${tableName}`);
		console.log(`   ✅ 列数: ${data.columns.length}`);
		console.log(`   ✅ 主键: ${data.primaryKeys.length > 0 ? data.primaryKeys.join(', ') : '无'}`);
		console.log(`   ✅ 索引: ${data.indexes.length}`);
		console.log(`   ✅ 外键: ${data.foreignKeys.length} ${data.foreignKeys.length > 0 ? '(新增功能验证通过!)' : ''}`);
	});

	await testSection('3.1 dm_describe_table - 不存在的表', async () => {
		const result = await client.callTool('dm_describe_table', {table_name: 'NONEXISTENT_TABLE_12345'});
		assertError(result, '不存在');
		console.log('   ✅ 正确返回错误: 表不存在');
	});

	await testSection('3.2 dm_describe_table - 非法表名（SQL 注入防护）', async () => {
		const result = await client.callTool('dm_describe_table', {table_name: "users; DROP TABLE users"});
		assertError(result, '无效');
		console.log('   ✅ 正确拦截: 非法表名');
	});

	// ==================== 4. dm_sample_data 测试 ====================
	await testSection('4. dm_sample_data - 查看表数据样本', async () => {
		const listResult = assertSuccess(await client.callTool('dm_list_tables'));
		const tableName = listResult.tables[0].name;

		const result = await client.callTool('dm_sample_data', {table_name: tableName, limit: 5});
		const data = assertSuccess(result);
		assert(data.tableName === tableName, '表名应匹配');
		assert(data.rowCount <= 5, '返回行数应 <= limit');
		assert(Array.isArray(data.fields), '应有 fields 字段');
		assert(Array.isArray(data.rows), '应有 rows 字段');
		console.log(`   ✅ 表: ${tableName}`);
		console.log(`   ✅ 返回 ${data.rowCount} 行，${data.fields.length} 列`);
		console.log(`   ✅ 字段: ${data.fields.join(', ')}`);
	});

	await testSection('4.1 dm_sample_data - limit 边界值', async () => {
		const listResult = assertSuccess(await client.callTool('dm_list_tables'));
		const tableName = listResult.tables[0].name;

		// limit = 0 应修正为 1
		const r1 = assertSuccess(await client.callTool('dm_sample_data', {table_name: tableName, limit: 0}));
		assert(r1.rowCount <= 1, 'limit=0 应修正为 1');
		console.log('   ✅ limit=0 → 修正为 1');

		// limit = -1 应修正为 1
		const r2 = assertSuccess(await client.callTool('dm_sample_data', {table_name: tableName, limit: -1}));
		assert(r2.rowCount <= 1, 'limit=-1 应修正为 1');
		console.log('   ✅ limit=-1 → 修正为 1');

		// limit = 9999 应限制为 1000
		const r3 = assertSuccess(await client.callTool('dm_sample_data', {table_name: tableName, limit: 9999}));
		assert(r3.rowCount <= 1000, 'limit=9999 应限制为 1000');
		console.log('   ✅ limit=9999 → 限制为 1000');
	});

	// ==================== 5. dm_query 测试 ====================
	await testSection('5. dm_query - 执行只读 SQL', async () => {
		const result = await client.callTool('dm_query', {sql: 'SELECT 1+1 AS result FROM DUAL'});
		const data = assertSuccess(result);
		assert(data.rowCount === 1, '应返回 1 行');
		assert(data.fields.length === 1, '应有 1 个字段');
		console.log(`   ✅ 简单查询成功: 1+1 = ${Object.values(data.rows[0])[0]}`);
	});

	await testSection('5.1 dm_query - 查询系统表', async () => {
		const result = await client.callTool('dm_query', {
			sql: "SELECT TABLE_NAME FROM USER_TAB_COMMENTS WHERE TABLE_TYPE='TABLE' AND ROWNUM <= 3",
		});
		const data = assertSuccess(result);
		assert(data.rowCount <= 3, '返回行数应 <= 3');
		console.log(`   ✅ 查询系统表: 返回 ${data.rowCount} 行`);
	});

	await testSection('5.2 dm_query - 行数限制', async () => {
		const result = await client.callTool('dm_query', {
			sql: "SELECT TABLE_NAME FROM USER_TAB_COMMENTS WHERE TABLE_TYPE='TABLE'",
			max_rows: 2,
		});
		const data = assertSuccess(result);
		assert(data.rowCount <= 2, '返回行数应 <= max_rows');
		console.log(`   ✅ max_rows=2: 返回 ${data.rowCount} 行，截断=${data.isTruncated}`);
	});

	await testSection('5.3 dm_query - 拒绝写操作', async () => {
		await client.callTool('dm_query', {sql: 'DELETE FROM test WHERE 1=1'}).catch(() => {});
		const result = await client.callTool('dm_query', {sql: 'DELETE FROM test'});
		assertError(result);
		console.log('   ✅ 正确拦截: DELETE 语句');
	});

	await testSection('5.4 dm_query - 拒绝 INSERT', async () => {
		const result = await client.callTool('dm_query', {sql: "INSERT INTO test VALUES (1)"});
		assertError(result);
		console.log('   ✅ 正确拦截: INSERT 语句');
	});

	await testSection('5.5 dm_query - 拒绝 UPDATE', async () => {
		const result = await client.callTool('dm_query', {sql: "UPDATE test SET name='x'"});
		assertError(result);
		console.log('   ✅ 正确拦截: UPDATE 语句');
	});

	await testSection('5.6 dm_query - 拒绝多语句（分号）', async () => {
		const result = await client.callTool('dm_query', {sql: "SELECT 1; DROP TABLE test"});
		assertError(result, '分号');
		console.log('   ✅ 正确拦截: 分号多语句');
	});

	await testSection('5.7 dm_query - 拒绝非 SELECT 前缀', async () => {
		const result = await client.callTool('dm_query', {sql: 'EXEC sp_help'});
		assertError(result);
		console.log('   ✅ 正确拦截: 非只读前缀');
	});

	// ==================== 6. Resource 测试 ====================
	await testSection('6. Resource: schema://tables - 自动发现表列表', async () => {
		const result = await client.readResource('schema://tables');
		assert(result.contents.length === 1, '应返回 1 个 content');
		const data = JSON.parse(result.contents[0].text);
		assert(data.totalCount > 0, '表数量应大于 0');
		assert(Array.isArray(data.tables), '应有 tables 字段');
		assert(data.tables[0].name, '表应包含 name');
		console.log(`   ✅ 自动发现 ${data.totalCount} 个表/视图`);
		console.log(`   ✅ 包含类型字段: ${data.tables[0].type}`);
	});

	await testSection('6.1 Resource: table://{name} - 自动发现表结构', async () => {
		const listData = JSON.parse(
			(await client.readResource('schema://tables')).contents[0].text
		);
		const tableName = listData.tables[0].name;

		const result = await client.readResource(`table://${tableName}`);
		assert(result.contents.length === 1, '应返回 1 个 content');
		const data = JSON.parse(result.contents[0].text);
		assert(data.tableName === tableName, '表名应匹配');
		assert(Array.isArray(data.columns), '应有 columns 字段');
		assert(Array.isArray(data.foreignKeys), '应有 foreignKeys 字段');
		console.log(`   ✅ Resource 获取表结构: ${tableName}`);
		console.log(`   ✅ ${data.columns.length} 列, ${data.foreignKeys.length} 个外键`);
	});

	await testSection('6.2 Resource: table://{name} - 不存在的表', async () => {
		const result = await client.readResource('table://NONEXISTENT_12345');
		const text = result.contents[0].text;
		assert(text.includes('不存在'), '应返回不存在的错误');
		console.log('   ✅ 正确返回: 表不存在');
	});

	// ==================== 总结 ====================
	console.log('\n' + BOLD + '═══════════════════════════════════════════════' + RESET);
	console.log(BOLD + '📊 测试结果汇总' + RESET);
	console.log(BOLD + '═══════════════════════════════════════════════' + RESET);

	const total = results.passed + results.failed + results.skipped;
	const passRate = total > 0 ? ((results.passed / total) * 100).toFixed(1) : 0;

	console.log(`   ${GREEN}✅ 通过: ${results.passed}${RESET}`);
	console.log(`   ${RED}❌ 失败: ${results.failed}${RESET}`);
	console.log(`   ${YELLOW}⏭️  跳过: ${results.skipped}${RESET}`);
	console.log(`   📈 通过率: ${passRate}%`);

	if (results.failed > 0) {
		console.log('\n' + RED + '失败用例:' + RESET);
		results.tests.filter(t => t.status === 'failed').forEach(t => {
			console.log(`   ❌ ${t.name}: ${t.error}`);
		});
	}

	console.log('\n' + (results.failed === 0 ? GREEN + '🎉 所有测试通过！' + RESET : RED + '⚠️  存在失败用例' + RESET) + '\n');
}

async function testSection(name, testFn) {
	process.stdout.write(CYAN + `  📋 ${name}... ` + RESET);
	try {
		await testFn();
		results.passed++;
		results.tests.push({name, status: 'passed'});
	} catch (err) {
		results.failed++;
		results.tests.push({name, status: 'failed', error: err.message});
		console.log(RED + '❌ 失败: ' + err.message + RESET);
	}
}

// 主函数
async function main() {
	const client = new McpTestClient();
	try {
		console.log('🚀 启动 MCP Server...');
		await client.start();
		await runTests(client);
	} catch (err) {
		console.error(RED + '💥 测试异常: ' + RESET + err.message);
		console.error(err.stack);
		process.exitCode = 1;
	} finally {
		client.stop();
	}
}

main();

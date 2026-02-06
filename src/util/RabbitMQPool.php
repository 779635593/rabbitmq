<?php

namespace zhuoxin\rabbitmq\util;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use Swoole\ConnectionPool;

/**
 * RabbitMQ 连接池
 * 注：RabbitMQ连接池中使用的是连接，不可复用通道充当连接池，协程中以连接为句柄，多个通道在协程中调用会报文件句柄错误。
 * 基于swoole通用连接池ConnectionPool，原始连接池，基于 Channel 自动调度，支持传入任意构造器 (callable)，构造器需返回一个连接对象
 * https://wiki.swoole.com/zh-cn/#/coroutine/conn_pool?id=connectionpool
 * get 方法获取连接（连接池未满时会创建新的连接）
 * put 方法回收连接
 * fill 方法填充连接池（提前创建连接）
 * close 关闭连接池
 */
class RabbitMQPool
{

	/**
	 * 获取连接池
	 *
	 * @param  int    $poolSize  // 连接池容量
	 * @param  bool   $isFill    // 提前创建连接
	 * @param  array  $config    // RabbitMQ配置
	 *
	 * @return ConnectionPool
	 */
	public static function getPool(int $poolSize = 64, bool $isFill = true, array $config = []): ConnectionPool
	{
		// RabbitMQ 默认配置
		$defaultConfig = [
			'host' => 'localhost',
			'port' => 5672,
			'user' => 'guest',
			'pass' => 'guest',
		];
		// 合并配置
		$config = array_merge($defaultConfig, $config);
		// 创建连接函数
		$createConnection = function () use ($config): AMQPStreamConnection {
			return new AMQPStreamConnection(
				$config['host'],
				$config['port'],
				$config['user'],
				$config['pass']
			);
		};

		// 实例化连接池
		$connectionPool = new ConnectionPool($createConnection, $poolSize);
		// 提前创建连接
		if ($isFill) {
			$connectionPool->fill();
		}

		return $connectionPool;
	}

}
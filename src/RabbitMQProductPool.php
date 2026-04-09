<?php

namespace zhuoxin\rabbitmq;

use zhuoxin\rabbitmq\util\RabbitMQChannel;
use zhuoxin\rabbitmq\util\RabbitMQPool;
use zhuoxin\rabbitmq\util\RabbitMQUtil;

// RabbitMQ消息生产者(连接池方式)
class RabbitMQProductPool
{

	// 连接池
	private \Swoole\ConnectionPool $connectionPool;

	/**
	 * RabbitMQ消息生产者(连接池方式)
	 *
	 * @param  int    $poolSize  // 连接池容量
	 * @param  bool   $isFill    // 提前创建连接
	 * @param  array  $config    // RabbitMQ配置
	 *
	 * @throws \Exception
	 */
	public function __construct(int $poolSize = 64, bool $isFill = true, array $config = [])
	{
		try {
			// 连接池
			$this->connectionPool = RabbitMQPool::getPool($poolSize, $isFill, $config);
		} catch (\Throwable $e) {
			throw new \Exception('实例化MQ生产者连接池错误：' . $e->getMessage());
		}
	}

	/**
	 * 发送消息
	 *
	 * @param  string  $exchangeName     // 交换机
	 * @param  string  $routingKey       // 路由key
	 * @param          $message          // 消息内容（推荐字符串格式，数组自动转json_encode）
	 * @param  array   $msgHeaders       // 额外消息头属性（可选）
	 * @param  array   $extraProperties  // 额外消息属性（可选）
	 * @param  float   $timeout          // 发布确认超时时间（默认 5 秒）
	 *
	 * @return bool|null
	 * @throws \Exception
	 */
	public function sendMessage(string $exchangeName, string $routingKey, $message, array $msgHeaders = [], array $extraProperties = [], float $timeout = 5.0): ?bool
	{
		$rabbitMQChannel = null;
		$channel         = null;
		$connection      = null;
		$sendResult      = null;
		try {
			// 获取连接
			$connection = $this->connectionPool->get();
			// 通道类,外部传入连接
			$rabbitMQChannel = new RabbitMQChannel([], $connection);
			// 获取rabbitMQ通道
			$channel      = $rabbitMQChannel->getChannel();
			$rabbitMQUtil = new RabbitMQUtil($channel);
			$sendResult   = $rabbitMQUtil->sendMessage($exchangeName, $routingKey, $message, $msgHeaders, $extraProperties, $timeout);
		} catch (\Throwable $e) {
			throw new \Exception('MQ发送消息错误：' . $e->getMessage());
		} finally {
			// 关闭通道
			if ($rabbitMQChannel && $channel) {
				$rabbitMQChannel->closeChannel($channel);
			}
			// 连接归还到池
			if ($connection) {
				$this->connectionPool->put($connection);
			}
		}

		return $sendResult;
	}

	/**
	 * 关闭连接池
	 *
	 * @return void
	 */
	public function closePoll()
	{
		try {
			$this->connectionPool->close();
		} catch (\Throwable $e) {
			echo 'MQ关闭连接池异常：' . $e->getMessage();
		}
	}

	public function __destruct()
	{
		$this->closePoll();
	}

}
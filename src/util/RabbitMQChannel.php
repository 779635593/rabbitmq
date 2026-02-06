<?php

namespace zhuoxin\rabbitmq\util;

use PhpAmqpLib\{
	Channel\AMQPChannel,
	Connection\AMQPStreamConnection
};

// RabbitMQ通道类
class RabbitMQChannel
{

	// RabbitMQ 默认配置
	private $config = [
		'host' => 'localhost',
		'port' => 5672,
		'user' => 'guest',
		'pass' => 'guest',
	];

	/**
	 * 对象内部连接
	 *
	 * @var AMQPStreamConnection
	 */
	private $connection = null;

	/**
	 * 通道类
	 *
	 * @param  array                      $config      // RabbitMQ配置
	 * @param  AMQPStreamConnection|null  $connection  // RabbitMQ连接(通常是连接池取出的连接)，为空则对象内部创建连接
	 *
	 * @throws \Exception
	 */
	public function __construct(array $config = [], AMQPStreamConnection $connection = null)
	{
		$this->config = array_merge($this->config, $config);
		// 外部传入连接
		if ( ! is_null($connection)) {
			$this->connection = $connection;
		}
	}

	/**
	 * 获取Channel通道
	 *
	 * @param  bool  $confirmSelect  // 发布确认状态
	 *
	 * @return AMQPChannel
	 * @throws \Exception
	 */
	public function getChannel(bool $confirmSelect = true): AMQPChannel
	{
		// 获取连接
		$conn = $this->getConnection();
		try {
			$channel = $conn->channel();
			// 发布确认状态
			if ($confirmSelect) {
				$channel->confirm_select();
			}
		} catch (\Throwable $e) {
			$this->throwException('获取通道失败:' . $e->getMessage());
		}

		return $channel;
	}

	/**
	 * 关闭Channel通道
	 *
	 * @param  AMQPChannel  $channel  // 通道
	 *
	 * @return void
	 * @throws \Exception
	 */
	public function closeChannel(AMQPChannel $channel)
	{
		if ( ! is_null($channel) && $channel->is_open()) {
			try {
				$channel->close();
			} catch (\Throwable $e) {
				$this->throwException("关闭通道失败:" . $e->getMessage());
			}
		}
	}

	/**
	 * 获取 RabbitMQ 连接
	 *
	 * @return AMQPStreamConnection|null
	 * @throws \Exception
	 */
	private function getConnection(): ?AMQPStreamConnection
	{
		// 实例内单例：避免重复创建连接
		if (is_null($this->connection) || ($this->connection && ! $this->connection->isConnected())) {
			try {
				$this->connection = new AMQPStreamConnection(
					$this->config['host'],
					$this->config['port'],
					$this->config['user'],
					$this->config['pass']
				);
			} catch (\Throwable $e) {
				$this->throwException('创建RabbitMQ连接失败:' . $e->getMessage());
			}
			echo '连接----';
		}

		return $this->connection;
	}

	/**
	 * 关闭 RabbitMQ 连接
	 *
	 * @return void
	 * @throws \Exception
	 */
	public function closeConnection()
	{
		if ( ! is_null($this->connection) && $this->connection->isConnected()) {
			try {
				$this->connection->close();
				// 重置实例连接属性
				$this->connection = null;
			} catch (\Throwable $e) {
				$this->throwException("关闭连接失败:" . $e->getMessage());
			}
		}
	}

	/**
	 * 抛出异常
	 *
	 * @throws \Exception
	 */
	private function throwException($message)
	{
		throw new \Exception($message);
	}

}
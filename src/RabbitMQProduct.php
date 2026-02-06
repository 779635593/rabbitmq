<?php

namespace zhuoxin\rabbitmq;

use zhuoxin\rabbitmq\util\RabbitMQChannel;
use zhuoxin\rabbitmq\util\RabbitMQUtil;

// RabbitMQ消息生产者
class RabbitMQProduct
{

	// 交换机
	private $exchangeName;

	// 路由key
	private $routingKey;

	// 通道类
	private $rabbitMQChannel;

	// 通道
	private $channel;

	// RabbitMQ工具类
	private $rabbitMQUtil;

	/**
	 * RabbitMQ消息生产者
	 *
	 * @param  string  $exchangeName  // 交换机
	 * @param  string  $routingKey    // 路由key
	 * @param  array   $config        // RabbitMQ配置
	 *
	 * @throws \Exception
	 */
	public function __construct(string $exchangeName, string $routingKey, array $config = [])
	{
		try {
			$this->exchangeName = $exchangeName;
			$this->routingKey   = $routingKey;
			// 通道类 内部创建连接
			$this->rabbitMQChannel = new RabbitMQChannel($config);
			// 获取rabbitMQ通道
			$this->channel = $this->rabbitMQChannel->getChannel();
			// rabbitMQ工具实例
			$this->rabbitMQUtil = new RabbitMQUtil($this->channel);
		} catch (\Throwable $e) {
			throw new \Exception('实例化MQ生产者错误：' . $e->getMessage());
		}
	}

	/**
	 * 发送消息
	 *
	 * @param         $message          // 消息内容（推荐字符串格式，数组自动转json_encode）
	 * @param  int    $delaySeconds     // 延迟时间（秒）,（大于0时设置消息延迟时间【交换机需是延迟类型，否则为即时消息】）
	 * @param  array  $msgHeaders       // 额外消息头属性（可选）
	 * @param  array  $extraProperties  // 额外消息属性（可选）
	 * @param  float  $timeout          // 发布确认超时时间（默认 5 秒）
	 *
	 * @return bool|null
	 * @throws \Exception
	 */
	public function sendMessage($message, int $delaySeconds = 0, array $msgHeaders = [], array $extraProperties = [], float $timeout = 5.0): ?bool
	{
		try {
			// 延迟时间（秒）,（大于0时则设置的消息延迟时间【交换机需是延迟类型，否则为即时消息】）
			if (max(0, $delaySeconds) > 0) {
				return $this->rabbitMQUtil->sendMessageDelay($this->exchangeName, $this->routingKey, $message, $delaySeconds, $msgHeaders, $extraProperties, $timeout);
			} else {
				return $this->rabbitMQUtil->sendMessage($this->exchangeName, $this->routingKey, $message, $msgHeaders, $extraProperties, $timeout);
			}
		} catch (\Throwable $e) {
			throw new \Exception('MQ发送消息错误：' . $e->getMessage());
		}
	}

	// 关闭连接 通道
	public function __destruct()
	{
		try {
			// 关闭连接 通道
			$this->rabbitMQChannel->closeChannel($this->channel);
			$this->rabbitMQChannel->closeConnection();
		} catch (\Throwable $e) {
			echo 'MQ关闭连接 通道异常：' . $e->getMessage();
		}
	}

}
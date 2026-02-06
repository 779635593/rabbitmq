<?php

namespace zhuoxin\rabbitmq\util;

use PhpAmqpLib\{
	Channel\AMQPChannel,
	Message\AMQPMessage,
	Wire\AMQPTable
};

// RabbitMQ工具类
// 步骤：
//      1获取通道类（如需连接池，先获取连接池中的连接，再将连接注入到通道类）
//      2获取该工具，注入步骤1中获取到的通道
//      3通道类中销毁通道
//      4通道类中关闭连接（连接池归还连接，无需关闭连接）
class RabbitMQUtil
{

	/**
	 * Channel通道
	 *
	 * @var AMQPChannel
	 */
	private $channel;

	/**
	 * @param  AMQPChannel  $channel  // 通道
	 */
	public function __construct(AMQPChannel $channel)
	{
		$this->channel = $channel;
	}

	/**
	 * 声明交换机
	 *
	 * @param  string  $exchangeName  // 交换机名
	 * @param  string  $type          // 交换机类型（默认 direct，支持 topic/fanout）
	 * @param  bool    $durable       // 是否持久化（默认 true）
	 *
	 * @return void
	 * @throws \Exception
	 */
	public function declareExchange(string $exchangeName, string $type = 'direct', bool $durable = true)
	{
		// 获取通道
		$channel = $this->channel;
		$channel->exchange_declare(
			$exchangeName,
			$type,
			false,
			$durable,
			false
		);
	}

	/**
	 * 声明延迟交换机（x-delayed-message 类型）
	 *
	 * @param  string  $exchangeName  // 交换机名
	 * @param  string  $delayedType   // 底层交换机类型（默认 direct，支持 topic/fanout）
	 * @param  bool    $durable       // 是否持久化（默认 true）
	 *
	 * @return void
	 * @throws \Exception
	 */
	public function declareExchangeDelay(string $exchangeName, string $delayedType = 'direct', bool $durable = true)
	{
		// 获取通道
		$channel   = $this->channel;
		$delayArgs = new AMQPTable([
			                           'x-delayed-type' => $delayedType,
		                           ]);
		// 声明延迟交换机
		$channel->exchange_declare(
			$exchangeName,
			'x-delayed-message',
			false,
			$durable,
			false,
			false,
			false,
			$delayArgs
		);
	}

	/**
	 * 声明队列并与交换机绑定
	 *
	 * @param  string  $queueName     // 队列名
	 * @param  string  $exchangeName  // 交换机名
	 * @param  string  $routingKey    // 路由键
	 * @param  bool    $durable       // 队列是否持久化（默认 true）
	 *
	 * @return void
	 * @throws \Exception
	 */
	public function declareQueueAndBind(string $queueName, string $exchangeName, string $routingKey, bool $durable = true)
	{
		// 获取通道
		$channel = $this->channel;
		// 声明队列
		$channel->queue_declare(
			$queueName,
			false,
			$durable,
			false,
			false
		);
		// 队列与交换机绑定
		$channel->queue_bind($queueName, $exchangeName, $routingKey);
	}

	/**
	 * 发送消息（带发布确认）
	 * 设置消息头属性,追加 重试次数 retry_count 属性(默认0，可覆盖)
	 *
	 * @param  string  $exchangeName     // 交换机名
	 * @param  string  $routingKey       // 路由键
	 * @param          $message          // 消息内容（推荐字符串格式，数组自动转json_encode）
	 * @param  array   $msgHeaders       // 额外消息头属性（可选）
	 * @param  array   $extraProperties  // 额外消息属性（可选）
	 * @param  float   $timeout          // 发布确认超时时间（默认 5 秒）
	 *
	 * @return true|void
	 * @throws \Exception
	 */
	public function sendMessage(string $exchangeName, string $routingKey, $message, array $msgHeaders = [], array $extraProperties = [], float $timeout = 5.0)
	{
		// 获取通道
		$channel = $this->channel;
		try {
			// 消息内容数组转json字符串
			if (is_array($message)) {
				$message = json_encode($message, JSON_UNESCAPED_UNICODE);
			}
			// 基础消息属性
			$msgProperties = array_merge([
				                             // （持久化）
				                             'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
			                             ], $extraProperties);
			// 创建消息
			$AMQPMessage = new AMQPMessage($message, $msgProperties);
			$AMQPMessage->set("application_headers", new AMQPTable($msgHeaders));
			// 发布消息
			$channel->basic_publish($AMQPMessage, $exchangeName, $routingKey);
			// 等待发布确认
			$channel->wait_for_pending_acks($timeout);

			return true;
		} catch (\Throwable $e) {
			$this->throwException("发送失败:" . $e->getMessage());
		}
	}

	/**
	 * 发送延迟消息
	 *
	 * @param  string  $exchangeName     // 交换机名
	 * @param  string  $routingKey       // 路由键
	 * @param          $message          // 消息内容（推荐字符串格式，数组自动转json_encode）
	 * @param  int     $delaySeconds     // 延迟时间（秒）【交换机需是延迟类型，否则为即时消息】
	 * @param  array   $msgHeaders       // 额外消息头属性（可选）
	 * @param  array   $extraProperties  // 额外消息属性（可选）
	 * @param  float   $timeout          // 发布确认超时时间（默认 5 秒）
	 *
	 * @return true|null
	 * @throws \Exception
	 */
	public function sendMessageDelay(string $exchangeName, string $routingKey, $message, int $delaySeconds, array $msgHeaders = [], array $extraProperties = [], float $timeout = 5.0): ?bool
	{
		// 设置消息头属性,追加延迟属性(不可覆盖)
		$msgHeaders = array_merge($msgHeaders, ['x-delay' => $delaySeconds * 1000]);

		return $this->sendMessage($exchangeName, $routingKey, $message, $msgHeaders, $extraProperties, $timeout);
	}

	/**
	 * 启动消费者监听（带手动 ACK、公平调度）
	 *  $callback = function (AMQPMessage $msg) {
	 *       echo '开始处理...';
	 *       // 队列名
	 *       echo '队列名：', $msg->getRoutingKey(), "\n";
	 *       $data = json_decode($msg->getBody(), true);
	 *       var_dump($data);
	 *       // 确认ack
	 *       $msg->ack();
	 *       // 处理失败，重新入列
	 *       // $msg->nack(true);
	 *  };
	 *
	 * @param  string    $queueName      // 队列名
	 * @param  callable  $callback       // 业务处理回调函数
	 * @param  int       $prefetchCount  // 预取数（默认 1，公平调度）
	 *
	 * @return void
	 * @throws \Exception
	 */
	public function startConsumer(string $queueName, callable $callback, int $prefetchCount = 1)
	{
		// 获取通道
		$channel = $this->channel;
		try {
			// 公平调度：只给空闲消费者分发新消息
			$channel->basic_qos(0, $prefetchCount, false);
			// 注册消费回调（手动 ACK 模式）
			$channel->basic_consume(
				$queueName,
				'',
				false,
				false, // no_ack = false 开启手动 ACK
				false,
				false,
				$callback
			);
			// 开始监听消费
			$channel->consume();
		} catch (\Throwable $e) {
			$this->throwException("消费监听异常：" . $e->getMessage());
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
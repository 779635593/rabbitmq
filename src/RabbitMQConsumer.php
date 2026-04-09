<?php

namespace zhuoxin\rabbitmq;

use PhpAmqpLib\Message\AMQPMessage;
use zhuoxin\rabbitmq\util\RabbitMQChannel;
use zhuoxin\rabbitmq\util\RabbitMQUtil;

// RabbitMQ消息消费者
// 业务与MQ解耦
class RabbitMQConsumer
{

	// 通道类
	private RabbitMQChannel $rabbitMQChannel;

	/**
	 * 创建通道连接
	 *
	 * @param  array  $config  // RabbitMQ配置
	 *
	 * @throws \Exception
	 */
	public function __construct(array $config = [])
	{
		try {
			// 通道类 内部创建连接
			$this->rabbitMQChannel = new RabbitMQChannel($config);
		} catch (\Throwable $e) {
			throw new \Exception('实例化MQ消费者错误：创建通道连接异常：' . $e->getMessage());
		}
	}

	/**
	 * 启动MQ消费监听
	 *   1.交换机、队列声明和绑定
	 *   2.错误重试，超过重试次数后丢弃
	 *
	 * @param  string    $exchangeName      // 交换机名
	 * @param  string    $routingKey        // 路由key
	 * @param  string    $queueName         // 队列名
	 * @param  callable  $businessCallback  // 业务逻辑回调,回调参数：1 消息数据，参数2 重试次数
	 * @param  array     $delayQueue        // 延迟队列配置，如有配置则创建延迟队列，ttl到期后，转发到死信队列（即当前方法设置的队列中）
	 * @param  int       $maxRetryCount     // 最大重试消息次数
	 * @param  string    $type              // 交换机类型（默认 direct，支持 topic/fanout）
	 *
	 *
	 * @return void
	 */
	public function startConsume(string   $exchangeName,
	                             string   $routingKey,
	                             string   $queueName,
	                             callable $businessCallback,
	                             array    $delayQueue = [],
	                             int      $maxRetryCount = 2,
	                             string   $type = 'direct'

	) {
		try {
			// 1. 获取MQ通道
			$channel  = $this->rabbitMQChannel->getChannel();
			$rabbitMQ = new RabbitMQUtil($channel);
			$this->dump('队列信息:');
			$this->dump("交换机     :" . $exchangeName);
			$this->dump("队列名     :" . $queueName);
			$this->dump("路由key    :" . $routingKey);

			// 2. 声明交换机、队列+绑定
			// 2.1 设置交换机
			$rabbitMQ->declareExchange($exchangeName, $type);
			$this->dump('声明交换机 :Success');

			// 2.2 队列绑定
			$rabbitMQ->declareQueueAndBind($queueName, $exchangeName, $routingKey);
			$this->dump('队列绑定   :Success');

			// 2.3 延迟队列配置,到期后转到死信队列中（2.2中的队列）
			if ( ! empty($delayQueue)) {
				// 延迟队列名
				$delayQueueName = $delayQueue['queueName'];
				// 延迟路由key
				$delayRoutingKey = $delayQueue['routingKey'];
				$this->dump("延迟队列名 :" . $delayQueueName);
				$this->dump("延迟路由key:" . $delayRoutingKey);
				// 延迟队列配置
				$delayQueueConfig = $delayQueue['queueConfig'];
				if (empty($delayQueueConfig['ttl'])) {
					throw new \Exception('延迟队列超时时间ttl不能为空');
				}
				// 延迟队列 默认扩展参数
				$delayQueueDefaultArguments = [
					// TTL:过期时间,单位毫秒
					'x-message-ttl'             => $delayQueueConfig['ttl'] * 1000,
					// 关键安全配置：防止内存溢出
					// 队列最大消息数
					'x-max-length'              => 100000,
					// 溢出行为：
					// drop-head (默认) 删头加尾。丢弃队列最老的消息（队首），将新消息加入队尾。被丢弃的旧消息不进死信 日志、监控等允许丢失的非核心数据
					// reject-publish 拒收新客。直接拒绝生产者发布的新消息（返回 basic.nack）。被拒绝的新消息不进死信 需要生产者感知背压，进行重试或降级
					// reject-publish-dlx 新客转DLX。拒绝新消息，并将其路由到死信交换机。被拒绝的新消息进死信 订单/支付等核心业务，防止消息丢失
					'x-overflow'                => 'reject-publish-dlx',
					// 队列最大内存（512MB）
					'x-max-length-bytes'        => 536870912,
					// 死信路由（必须正确配置，否则消息变“僵尸”）
					// 死信交换机,当前交换机
					'x-dead-letter-exchange'    => $exchangeName,
					// 死信路由键，当前队列路由key
					'x-dead-letter-routing-key' => $routingKey,
				];
				// 合并延迟队列参数
				$delayQueueArguments = array_merge($delayQueueDefaultArguments, $delayQueueConfig);
				$rabbitMQ->declareQueueAndBind($delayQueueName, $exchangeName, $delayRoutingKey, $delayQueueArguments);
				$this->dump('延迟队列绑定:Success');
				$this->dump("延迟时间    :" . $delayQueueConfig['ttl'] . '秒');
			}

			// 3. 消费回调（核心：逻辑全在这里）
			$callback = function (AMQPMessage $AMQPMessage) use (
				$rabbitMQ,
				$exchangeName,
				$routingKey,
				$businessCallback,
				$maxRetryCount
			) {
				// 读取消息头中的重试次数
				$retryCount = 0;
				$msgHeaders = $AMQPMessage->get('application_headers');
				if ($msgHeaders) {
					$headersData = $msgHeaders->getNativeData();
					$retryCount  = $headersData['retry_count'] ?? 0;
				}
				// 获取消息内容，字符串格式
				$data = $AMQPMessage->getBody();
				try {
					// 业务逻辑回调,参数: 参数1 消息数据, 参数2 已重试次数
					call_user_func($businessCallback, $data, $retryCount);
				} catch (\Throwable $e) {
					// 捕获业务逻辑抛出的异常
					// 检测重试次数，重试次数+1
					if ($retryCount++ < $maxRetryCount) {
						$this->dump('重试次数:' . $retryCount);
						// 重发消息时携带重试次数
						$rabbitMQ->sendMessage($exchangeName, $routingKey, $data, ['retry_count' => $retryCount]);
					} else {
						// 重试次数超过上限
						$this->dump('重试次数已达上限');
					}
				} finally {
					// 无论成败，最终都ACK确认，使用重发消息带重试次数消息头进行重放
					$AMQPMessage->ack();
				}
			};
			// 4. 启动消费监听（固定逻辑）
			$this->dump('MQ消息监听中···');
			$rabbitMQ->startConsumer($queueName, $callback);
		} catch (\Throwable $e) {
			$this->dump('MQ消息监听异常:' . $e->getMessage());
		} finally {
			// 5. 最终关闭通道+连接
			if (isset($rabbitMQChannel) && isset($channel)) {
				$rabbitMQChannel->closeChannel($channel);
				$rabbitMQChannel->closeConnection();
			}
		}
	}

	// 打印输出
	private function dump($msg)
	{
		echo "-----▶" . $msg . PHP_EOL;
	}

}
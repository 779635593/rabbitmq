<?php

namespace zhuoxin\rabbitmq;

use PhpAmqpLib\Message\AMQPMessage;
use zhuoxin\rabbitmq\util\RabbitMQChannel;
use zhuoxin\rabbitmq\util\RabbitMQUtil;

// RabbitMQ消息消费者
// 业务与MQ解耦
class RabbitMQConsumer
{

	/**
	 * 通道类
	 *
	 * @var RabbitMQChannel
	 */
	private $rabbitMQChannel;

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
	 *  1.交换机、队列声明和绑定
	 *  2.错误重试，超过重试次数后丢弃
	 *
	 * @param  string    $exchangeName      // 交换机名
	 * @param  string    $routingKey        // 路由key
	 * @param  string    $queueName         // 队列名
	 * @param  callable  $businessCallback  // 业务逻辑回调,参数1 消息数据，参数2 重试次数
	 * @param  int       $delaySeconds      // 延迟时间（秒）,（大于0时2个作用:1.设置延迟类型交换机，2.重放时设置消息延迟时间）
	 * @param  int       $maxRetryCount     // 最大重试消息次数,默认2
	 * @param  string    $type              // 交换机类型（默认 direct，支持 topic/fanout）
	 *
	 * @return void
	 * @throws \Exception
	 */
	public function startConsume(string   $exchangeName,
	                             string   $routingKey,
	                             string   $queueName,
	                             callable $businessCallback,
	                             int      $delaySeconds = 0,
	                             int      $maxRetryCount = 2,
	                             string   $type = 'direct'

	) {
		try {
			// 1. 获取MQ通道
			$channel  = $this->rabbitMQChannel->getChannel();
			$rabbitMQ = new RabbitMQUtil($channel);
			$this->dump('队列信息:');
			$this->dump("交换机     :" . $exchangeName);
			$this->dump("路由key    :" . $routingKey);
			$this->dump("队列名     :" . $queueName);

			// 2. 声明交换机、队列+绑定
			// 2.1 延迟时间大于0 作用:1.设置延迟类型交换机
			if (max(0, $delaySeconds) > 0) {
				// 交换机类型
				$exchangeNameType = '延迟交换机';
				$rabbitMQ->declareExchangeDelay($exchangeName, $type);
			} else {
				$exchangeNameType = '即时交换机';
				$rabbitMQ->declareExchange($exchangeName, $type);
			}
			$this->dump("交换机类型 :" . $exchangeNameType);
			$this->dump('声明交换机 :Success');
			// 2.2 队列绑定
			$rabbitMQ->declareQueueAndBind($queueName, $exchangeName, $routingKey);
			$this->dump('队列绑定   :Success');
			// 3. 封装通用的消费回调（核心：固定逻辑全在这里）
			$callback = function (AMQPMessage $AMQPMessage) use (
				$rabbitMQ,
				$exchangeName,
				$routingKey,
				$businessCallback,
				$maxRetryCount,
				$delaySeconds
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
						// 延迟时间大于0 作用:2.重放时设置消息延迟时间
						if (max(0, $delaySeconds) > 0) {
							$rabbitMQ->sendMessageDelay($exchangeName, $routingKey, $data, $delaySeconds, ['retry_count' => $retryCount]);
						} else {
							$rabbitMQ->sendMessage($exchangeName, $routingKey, $data, ['retry_count' => $retryCount]);
						}
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
			$this->dump('MQ消息消费异常:' . $e->getMessage());
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
		echo "-----> " . $msg . PHP_EOL;
	}

}
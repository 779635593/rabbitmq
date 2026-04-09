<?php

require_once __DIR__ . '/vendor/autoload.php';

use Swoole\Coroutine\Barrier;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use zhuoxin\rabbitmq\RabbitMQProductPool;

use function Swoole\Coroutine\go;
use function Swoole\Coroutine\run;

// 发送消息
// 协程方式，前提安装 Swoole 拓展
Runtime::enableCoroutine();
run(function () {
	try {
		$startTime = microtime(true);
		// 协程速率
		$rateChannel = new Channel(5);
		// 协程屏障
		$barrier = Barrier::make();
		// RabbitMQ消息生产者(连接池方式)
		$rabbitMQProduct = new RabbitMQProductPool();

		$exchangeName = 'common.exchange';
		$routingKey   = 'common.test.route_key';
		$queueName    = 'common.test.queue';
		$delayQueue   = [
			'routingKey'  => 'common.test.delay_route_key',
			'queueName'   => 'common.test.delay_queue',
			'queueConfig' => [
				'ttl' => 6,
			],
		];
		// 延迟队列名
		$delayQueueName = $delayQueue['queueName'];
		// 延迟路由key
		$delayRoutingKey = $delayQueue['routingKey'];

		for ($i = 1000; $i--;) {
			$rateChannel->push(true);
			go(function () use (
				$i,
				$rateChannel,
				$barrier,
				$rabbitMQProduct,
				$exchangeName,
				$delayRoutingKey
			) {
				try {
					$data = [
						'time' => time(),
						'data' => '延迟订单号' . $i,
					];
					// 发送消息
					$res = $rabbitMQProduct->sendMessage($exchangeName, $delayRoutingKey, $data);
					echo "[$i] 发送结果：" . ($res ? '成功' : '失败') . PHP_EOL;
					$rateChannel->pop();
				} catch (Exception $e) {
					echo '异常';
					var_dump($e->getMessage());
				}
			});
		}
		// 所有子协程完成
		Barrier::wait($barrier);

		$endTime = microtime(true);
		echo "所有消息发送完成，总用时：" . number_format($endTime - $startTime, 3) . "秒" . PHP_EOL;
	} catch (\Exception $e) {
		var_dump($e->getMessage());
	}
});


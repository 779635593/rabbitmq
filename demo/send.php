<?php

require_once __DIR__ . '/vendor/autoload.php';

use zhuoxin\rabbitmq\RabbitMQProduct;

// 发送消息
try {
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

	$startTime = microtime(true);
	// RabbitMQ消息生产者
	$rabbitMQProduct = new RabbitMQProduct();

	// 即时消息
	for ($i = 0; $i < 2; $i++) {
		$data = [
			'time' => time(),
			'data' => '即时消息' . $i,
		];
		// 发送消息
		$res = $rabbitMQProduct->sendMessage($exchangeName, $routingKey, $data);
		echo "[$i] 发送结果：" . ($res ? '成功' : '失败') . PHP_EOL;
	}

	// 延迟消息
	for ($i = 0; $i < 2; $i++) {
		$data = [
			'time' => time(),
			'data' => '延迟消息' . $i,
		];
		// 发送消息
		$res = $rabbitMQProduct->sendMessage($exchangeName, $delayRoutingKey, $data);
		echo "[$i] 发送结果：" . ($res ? '成功' : '失败') . PHP_EOL;
	}


	$endTime = microtime(true);
	echo "所有消息发送完成，总用时：" . number_format($endTime - $startTime, 3) . "秒" . PHP_EOL;

	echo '关闭';
} catch (\Exception $e) {
	var_dump($e->getMessage());
}

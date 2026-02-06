<?php

require_once __DIR__ . '/vendor/autoload.php';

use zhuoxin\rabbitmq\RabbitMQProduct;

// 发送消息
try {
	// 交换机：延迟交换机
	$exchangeName = 'test_delay_exchange';
	// 交换机绑定的路由key：路由key
	$routingKey = 'test_delay_route_key';

	// RabbitMQ消息生产者
	$rabbitMQProduct = new RabbitMQProduct($exchangeName, $routingKey);

	for ($i = 0; $i < 100; $i++) {
		$data = [
			'time' => time(),
			'data' => '订单号' . 555 . $i,
		];
		// 延迟时间（秒）,（大于0时设置消息延迟时间【交换机需是延迟类型，否则为即时消息】）
		$delaySeconds = 2;
		// 发送消息
		$res = $rabbitMQProduct->sendMessage($data, $delaySeconds);
		echo '发送结果：';
		var_dump($res);
	}

	echo '关闭';
} catch (\Exception $e) {
	var_dump($e->getMessage());
}

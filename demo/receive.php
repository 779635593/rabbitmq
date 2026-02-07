<?php

require_once __DIR__ . '/vendor/autoload.php';

use zhuoxin\rabbitmq\RabbitMQConsumer;

// 声明延迟交换机
// 交换机：延迟交换机
$exchangeName = 'test_delay_exchange';
// 交换机绑定的路由key：路由key
$routingKey = 'test_delay_route_key';
// 队列
$queueName = 'test_delay_queue';

// 消费消息 延迟消息
// 业务逻辑回调,参数 消息数据
$businessCallback = function ($data) {
	try {
		var_dump('这是逻辑业务');
		var_dump(json_decode($data, true));
	} catch (\Throwable $e) {
		// 业务逻辑异常，捕获异常
		// 数据库回滚，写入异常日志等
		var_dump('捕获错误消息：' . $e->getMessage());
		// 手动抛出异常，用于消费者接收异常后消息重试。
		throw $e;
	}
};

try {
	// 启动消费
	$rabbitMQConsumer = new RabbitMQConsumer();
	$rabbitMQConsumer->startConsume($exchangeName,
	                                $routingKey,
	                                $queueName,
	                                $businessCallback,
	                                $delaySeconds = 2

	);
} catch (\Exception $e) {
	var_dump('消费者启动异常：' . $e->getMessage());
}

echo '关闭';
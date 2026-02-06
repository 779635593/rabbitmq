<?php

require_once __DIR__ . '/vendor/autoload.php';

use Swoole\Coroutine\Barrier;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use zhuoxin\rabbitmq\RabbitMQProductPool;

use function Swoole\Coroutine\go;
use function Swoole\Coroutine\run;

// 协程 发送消息
// 前提安装swoole拓展
Runtime::enableCoroutine();
run(function () {
	try {
		$startTime = microtime(true);
		// 协程速率
		$rateChannel = new Channel(50);
		// 协程屏障
		$barrier = Barrier::make();
		// 交换机：延迟交换机
		$exchangeName = 'test_delay_exchange';
		// 交换机绑定的路由key：路由key
		$routingKey = 'test_delay_route_key';
		// RabbitMQ消息生产者(连接池方式)
		$rabbitMQProduct = new RabbitMQProductPool($exchangeName, $routingKey);

		for ($i = 100; $i--;) {
			$rateChannel->push(true);
			go(function () use (
				$i,
				$rateChannel,
				$barrier,
				$rabbitMQProduct
			) {
				try {
					$data = [
						'time' => time(),
						'data' => '延迟订单号' . $i,
					];
					// 延迟时间（秒）,（大于0时设置消息延迟时间【交换机需是延迟类型，否则为即时消息】）
					$delaySeconds = 2;
					// 发送消息
					$res = $rabbitMQProduct->sendMessage($data, $delaySeconds);
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


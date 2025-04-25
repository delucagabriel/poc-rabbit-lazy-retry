import amqp from 'amqplib';

const RABBIT_URL = 'amqp://localhost';

const MAIN_QUEUE = 'process_queue';
const RETRY_QUEUE = 'process_queue.retry';
const DEAD_QUEUE = 'process_queue.dead';
const MAX_RETRIES = 5;

async function bootstrap(channel: amqp.Channel) {
  await channel.assertQueue(DEAD_QUEUE, { durable: true });
  await channel.assertQueue(RETRY_QUEUE, {
    durable: true,
    arguments: {
      'x-message-ttl': 5000,
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': MAIN_QUEUE
    }
  });
  await channel.assertQueue(MAIN_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': RETRY_QUEUE
    }
  });
}

async function processMain(channel: amqp.Channel) {
  channel.consume(MAIN_QUEUE, (msg) => {
    if (!msg) return;
    const content = msg.content.toString();
    const retries = (msg.properties.headers ?? {})['x-retries'] || 0;
    console.log(`Processing... "${content}" (attempt: ${retries + 1})`);
    const fail = true;
    if (fail) {
      if (retries < MAX_RETRIES) {
        console.error(`Error - redirect to retry queue: ${RETRY_QUEUE} - (${retries + 1}/${MAX_RETRIES})`);
        channel.sendToQueue(RETRY_QUEUE, Buffer.from(content), {
          headers: { 'x-retries': retries + 1 }
        });
      } else {
        console.log(`Max retries — send to final DLQ: ${DEAD_QUEUE}`);
        channel.sendToQueue(DEAD_QUEUE, Buffer.from(content), {
          headers: { 'x-retries': retries }
        });
      }
      channel.ack(msg);
    } else {
      console.log(`Processed with success`);
      channel.ack(msg);
    }
  }, { noAck: false });
}

async function run() {
  const conn = await amqp.connect(RABBIT_URL);
  const channel = await conn.createChannel();
  await bootstrap(channel);
  channel.sendToQueue(MAIN_QUEUE, Buffer.from('execute task'), {
    headers: { 'x-retries': 0 }
  });
  console.log('Sending message to process');
  
  processMain(channel);

  channel.consume(DEAD_QUEUE, (msg) => {
    if (msg) {
      console.log(`[FINAL DLQ]: ${msg.content.toString()}`);
      channel.ack(msg);
    }
  });

  console.log('Waiting for messages...');
}

run().catch(console.error);
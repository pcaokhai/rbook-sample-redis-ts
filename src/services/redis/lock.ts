import { randomBytes } from 'crypto';
import { client } from './client';

export const withLock = async (key: string, cb: (redisClient: Client, signal: any) => any) => {
	// variables to control retry behaviour
	const retryDelayMs = 100;
	let retries = 20;
	const timeoutMs = 2000;

	// Generate a random value to store at a lock key
	const token = randomBytes(6).toString('hex');

	// Create the lock key
	const lockKey = `lock:${key}`;

	// Set up the while loop to implement the retry
	while (retries >= 0) {
		retries--;
		// try to do the SET NX operation
		const acquired = await client.set(lockKey, token, {
			NX: true,
			PX: timeoutMs
		});
		// ELSE brief pause (retryDelayMs) and then retry
		if (!acquired) {
			await pause(retryDelayMs);
			continue;
		}

		// if the SET is successfull, then run the callback
		try {
			const signal = { expired: false };
			setTimeout(() => {
				signal.expired = true;
			}, timeoutMs);

			const proxiedClient = buildClientProxy(timeoutMs);
			const result = await cb(proxiedClient, signal);
			return result;
		} finally {
			// Unset the locked set
			await client.unlock(lockKey, token);
		}
	}
};

type Client = typeof client;

const buildClientProxy = (timeoutMs: number) => {
	const startTime = Date.now();

	const handler = {
		get(target: Client, prop: keyof Client) {
			if (Date.now() >= startTime + timeoutMs) {
				throw new Error('Lock has expired');
			}

			const value = target[prop];

			return typeof value === 'function' ? value.bind(target) : value;
		}
	};

	return new Proxy(client, handler) as Client;
};

const pause = (duration: number) => {
	return new Promise((resolve) => {
		setTimeout(resolve, duration);
	});
};

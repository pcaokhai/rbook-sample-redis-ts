import { client } from '$services/redis';

export const incrementView = async (itemId: string, userId: string) => {
	// const inserted = await client.pfAdd(itemsViewKey(itemId), userId);

	// if (inserted) {
	// 	return Promise.all([
	// 		client.hIncrBy(itemsKey(itemId), 'views', 1),
	// 		client.zIncrBy(itemsByViewKey(), 1, itemId)
	// 	]);
	// }

	return client.incrementView(itemId, userId);
};

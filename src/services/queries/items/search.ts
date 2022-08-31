import { client } from '$services/redis';
import { deserialize } from './deserialize';
import { itemsIndexKey } from '$services/keys';

export const searchItems = async (term: string, size: number = 5) => {
	const cleaned = term
		.replaceAll(/[^a-zA-Z0-9 ]/g, '')
		.trim()
		.split(' ')
		.map((word) => (word ? `%${word}%` : ''))
		.join(' ');

	// look at the cleaned and make sure it is valid
	if (cleaned === '') {
		return [];
	}

	const quey = `(@name:(${cleaned}) => {$weight: 5.0}) | (@description:(${cleaned}))`;

	// Use the client to do an actual search
	const results = await client.ft.search(itemsIndexKey(), cleaned, {
		LIMIT: {
			from: 0,
			size
		}
	});

	// deserialize and return the search results
	return results.documents.map(({ id, value }) => deserialize(id, value as any));
};

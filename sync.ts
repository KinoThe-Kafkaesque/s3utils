import {
	S3Client,
	ListObjectsV2Command,
	HeadObjectCommand,
	CopyObjectCommand,
} from "@aws-sdk/client-s3";
import { writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";

import { config } from "dotenv";

config();
// Initialize AWS SDK
const s3Client = new S3Client({
	region: process.env.S3_LOCATION, // 'us-east-1'
	credentials: {
		accessKeyId: process.env.S3_ID as string,
		secretAccessKey: process.env.S3_SECRET as string,
	},
});

interface Cache {
	[key: string]: boolean; // The key is the object key, and the value indicates if it was copied (true) or not (false)
}

const cacheFilePath = join(__dirname, "s3CopyCache.json");

function setNestedObjectByPath(obj: any, path: string, value: any): void {
	const parts = path.split("/");
	let current = obj;

	for (let i = 0; i < parts.length; i++) {
		const part = parts[i];

		// If we're at the last part, set the value
		if (i === parts.length - 1) {
			current[part] = value;
			return;
		}

		// If the current part doesn't exist or isn't an object, create it
		if (!current[part] || typeof current[part] !== "object") {
			current[part] = {};
		}

		// Move down the structure
		current = current[part];
	}
}

function getNestedObjectValue(obj: any, path: string): any {
	const parts = path.split("/");
	let current = obj;

	for (const part of parts) {
		// If the part doesn't exist, return undefined
		if (!current[part]) {
			return undefined;
		}

		// Move down the structure
		current = current[part];
	}

	return current;
}
const readCache = (): Cache => {
	if (existsSync(cacheFilePath)) {
		try {
			const data = readFileSync(cacheFilePath, "utf-8");
			return JSON.parse(data) as Cache;
		} catch (error) {
			console.error("Error reading cache file:", error);
			return {};
		}
	}
	return {};
};

const writeCache = (cache: Cache) => {
	try {
		writeFileSync(cacheFilePath, JSON.stringify(cache, null, 2), "utf-8");
	} catch (error) {
		console.error("Error writing cache file:", error);
	}
};
const copyFolder = async (
	sourceBucket: string,
	targetBucket: string,
	folderPath: string,
	invalidateCache = false,
) => {
	const cache: Cache = readCache();

	const listParams = {
		Bucket: sourceBucket,
		Prefix: folderPath.endsWith("/") ? folderPath : `${folderPath}/`, // Ensure the folder path ends with a '/'
	};

	const transactionKey = `${sourceBucket}T${targetBucket}`;

	try {
		const listedObjects = await s3Client.send(
			new ListObjectsV2Command(listParams),
		);
		const diff: string[] = [];
		if (listedObjects.Contents) {
			for (const item of listedObjects.Contents) {
				const targetKey = item.Key ?? "";
				const cacheKey = `${transactionKey}/${item.Key}` ?? transactionKey;
				// Skip if already copied
				if (getNestedObjectValue(cache, cacheKey) === true) {
					console.log(
						`Object ${targetKey} already copied and cached. Skipping.`,
					);
					continue;
				}

				const headParams = {
					Bucket: targetBucket,
					Key: targetKey,
				};

				try {
					// Check if the object exists in the target bucket
					await s3Client.send(new HeadObjectCommand(headParams));
					console.log(
						`Object ${targetKey} already exists in ${targetBucket}. Skipping.`,
					);
					setNestedObjectByPath(cache, cacheKey, true);
					writeCache(cache);
				} catch (error: any) {
					if (error.name === "NotFound") {
						// Object does not exist in the target bucket, copy it
						const copySource = encodeURIComponent(
							`${sourceBucket}/${item.Key}`,
						);
						const copyParams = {
							Bucket: targetBucket,
							CopySource: copySource,
							Key: targetKey,
						};
						await s3Client.send(new CopyObjectCommand(copyParams));
						console.log(`Copied ${item.Key} to ${targetBucket}.`);
						// Update cache
						setNestedObjectByPath(cache, cacheKey, true);
						writeCache(cache);
					} else {
						// Log unexpected errors
						console.error(
							`Error checking object in target bucket: ${error.message}`,
						);
					}
				}
			}
		}
	} catch (error: any) {
		console.error(`Failed to copy folder: ${error.message}`);
	}
};

const sourceBucket = process.env.S3_BUCKET_DEV as string;
const targetBucket = process.env.S3_BUCKET_PROD as string;
const folderPath = "images";

copyFolder(sourceBucket, targetBucket, folderPath);

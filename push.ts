import { S3Client, ListObjectsV2Command, HeadObjectCommand, CopyObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { writeFileSync, readFileSync, existsSync, createWriteStream, mkdirSync, readdirSync, statSync } from 'fs';
import { dirname, join, resolve } from 'path';
import * as  crypto from 'crypto';
import { config } from 'dotenv';

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
    [key: string]: boolean | string; // The key is the object key, and the value indicates if it was copied (true) or not (false)
}

// const cacheFilePath = join(__dirname, 's3CopyCache.json');
const cacheFilePath = join(__dirname, 'localCache.json');


function setNestedObjectByPath(obj: any, path: string, value: any): void {
    const parts = path.split('/');
    let current = obj;

    for (let i = 0; i < parts.length; i++) {
        const part = parts[i];

        // If we're at the last part, set the value
        if (i === parts.length - 1) {
            current[part] = value;
            return;
        }

        // If the current part doesn't exist or isn't an object, create it
        if (!current[part] || typeof current[part] !== 'object') {
            current[part] = {};
        }

        // Move down the structure
        current = current[part];
    }
}

function getNestedObjectValue(obj: any, path: string): any {
    const parts = path.split('/');
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
            const data = readFileSync(cacheFilePath, 'utf-8');
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
        writeFileSync(cacheFilePath, JSON.stringify(cache, null, 2), 'utf-8');
    } catch (error) {
        console.error("Error writing cache file:", error);
    }
};
const copyFolder = async (sourceBucket: string, targetBucket: string, folderPath: string, invalidateCache = false) => {
    const cache: Cache = readCache();

    const listParams = {
        Bucket: sourceBucket,
        Prefix: folderPath.endsWith('/') ? folderPath : `${folderPath}/`, // Ensure the folder path ends with a '/'
    };

    const transactionKey = `${sourceBucket}T${targetBucket}`;

    try {
        const listedObjects = await s3Client.send(new ListObjectsV2Command(listParams));
        const diff: string[] = []
        if (listedObjects.Contents) {

            for (const item of listedObjects.Contents) {
                const targetKey = item.Key ?? '';
                const cacheKey = `${transactionKey}/${item.Key}` ?? transactionKey;
                // Skip if already copied
                if (getNestedObjectValue(cache, cacheKey) === true) {
                    console.log(`Object ${targetKey} already copied and cached. Skipping.`);
                    continue;
                }

                const headParams = {
                    Bucket: targetBucket,
                    Key: targetKey,
                };

                try {
                    // Check if the object exists in the target bucket
                    await s3Client.send(new HeadObjectCommand(headParams));
                    console.log(`Object ${targetKey} already exists in ${targetBucket}. Skipping.`);
                    setNestedObjectByPath(cache, cacheKey, true);
                    writeCache(cache);
                } catch (error: any) {
                    if (error.name === 'NotFound') {
                        // Object does not exist in the target bucket, copy it
                        const copySource = encodeURIComponent(`${sourceBucket}/${item.Key}`);
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
                        console.error(`Error checking object in target bucket: ${error.message}`);
                    }
                }
            }
        }


    } catch (error: any) {
        console.error(`Failed to copy folder: ${error.message}`);
    }
};

const downloadFolder = async (bucket: string, folderPath: string, localDir: string) => {
    const listParams = {
        Bucket: bucket,
        Prefix: folderPath.endsWith('/') ? folderPath : `${folderPath}/`, // Ensure the folder path ends with a '/'
    };

    try {
        const listedObjects = await s3Client.send(new ListObjectsV2Command(listParams));

        if (!listedObjects.Contents) {
            console.log("No objects found.");
            return;
        }

        for (const item of listedObjects.Contents) {
            const objectKey = item.Key ?? '';
            if (!objectKey) continue; // Skip if for some reason the object key is undefined
            if (objectKey.endsWith('/')) continue; // Skip directories
            const getParams = {
                Bucket: bucket,
                Key: objectKey,
            };
            const localFilePath = join(localDir, objectKey.replace(folderPath, ''));
            const localFileDir = dirname(localFilePath);
            console.log(localFileDir);

            try {
                // Stream the S3 object to a file
                const { Body } = await s3Client.send(new GetObjectCommand(getParams));
                if (Body) {
                    mkdirSync(localFileDir, { recursive: true });
                    const writeStream = createWriteStream(localFilePath);
                    (Body as NodeJS.ReadableStream).pipe(writeStream); // Force the type if absolutely necessary
                }
            } catch (downloadError) {
                console.error(`Failed to download ${objectKey}: ${downloadError}`);
            }
        }
    } catch (listError) {
        console.error(`Failed to list objects in folder '${folderPath}': ${listError}`);
    }
};

const pushFolderToS3 = async (localDir: string, bucket: string, s3Folder: string, invalidateCache = false) => {
    const cache: Cache = readCache();
    const localFiles = getFilesRecursively(localDir);
    const transactionKey = bucket;
    if (invalidateCache) {
        // Invalidate cache for the file
        setNestedObjectByPath(cache, transactionKey + '/' + s3Folder, '');
        writeCache(cache);
    }
    for (const localFilePath of localFiles) {
        const relativePath = localFilePath.replace(`${resolve(localDir)}/`, '');
        const s3Key = join(relativePath).replace(/\\/g, '/'); // Ensure proper S3 key format
        const fileHash = getFileHash(localFilePath);

        const cacheKey = `${transactionKey}/${s3Key}`;
        // Check cache to decide if the file needs to be uploaded
        if (!invalidateCache && getNestedObjectValue(cache, cacheKey) === fileHash) {
            console.log(`Skipping ${localFilePath}, already uploaded.`);
            continue;
        }

        try {
            // Upload file to S3
            const s3SavePath = s3Key.replace(localDir + '/', '');
            console.log(s3SavePath);
            console.log(localDir);


            const fileContent = readFileSync(localFilePath);
            await s3Client.send(new PutObjectCommand({
                Bucket: bucket,
                Key: s3SavePath,
                Body: fileContent,
            }));
            console.log(`Uploaded ${localFilePath} to ${s3SavePath}.`);

            // Update cache
            setNestedObjectByPath(cache, cacheKey, fileHash);
            writeCache(cache);
        } catch (error) {
            console.error(`Failed to upload ${localFilePath}:`, error);
        }
    }
};

const getFilesRecursively = (dir: string, fileList: string[] = []): string[] => {
    const files = readdirSync(dir);
    for (const file of files) {
        const fullPath = join(dir, file);
        if (statSync(fullPath).isDirectory()) {
            getFilesRecursively(fullPath, fileList);
        } else {
            fileList.push(fullPath);
        }
    }
    return fileList;
};

const getFileHash = (filePath: string): string => {
    const fileBuffer = readFileSync(filePath);
    const hashSum = crypto.createHash('sha256');
    hashSum.update(fileBuffer);
    return hashSum.digest('hex');
};

// Example usage:

const sourceBucket = process.env.S3_BUCKET_DEV as string;
const targetBucket = process.env.S3_BUCKET_PROD as string;
const folderPath = 'images';

const localDir = 'images'; // Local directory where the folder will be downloaded
// downloadFolder(sourceBucket, 'images', localDir);
// copyFolder(sourceBucket, targetBucket, folderPath);
pushFolderToS3(localDir, sourceBucket, folderPath);
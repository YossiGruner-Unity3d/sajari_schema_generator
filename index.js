const axios = require('axios');
require('dotenv').config()
var sleep = require('sleep');

var auth = {
    username: process.env.KEY_ID,
    password: process.env.KEY_SECRET
}

async function getCollectionsList() {
    try {
        const response = await axios.get('https://api-gateway.sajari.com/v4/collections',
            {
                auth: auth
            });
        return response.data;
    } catch (error) {
        console.error(error);
    }
}

async function createNewCollection(display_name) {
    axios.post('https://api-gateway.sajari.com/v4/collections?collection_id=' + display_name
        , {
            display_name: display_name
        },
        {
            auth: auth
        })
        .then(function (response) {
            return response.data;
        })
        .catch(function (error) {
            console.error(error);
        });
}

async function getCollectionsSchemaFields(collection_id) {
    try {
        const response = await axios.get('https://api-gateway.sajari.com/v4/collections/' + collection_id + '/schemaFields?page_size=10000',
            {
                auth: auth
            });
        return response.data.schema_fields;
    } catch (error) {
        console.error(error);
    }
}

async function createCollectionsSchemaFields(collection_id, schemaFieldsList) {
    try {
        const response = await axios.post('https://api-gateway.sajari.com/v4/collections/' + collection_id + '/schemaFields:batchCreate', {
            'fields': schemaFieldsList
        },
            {
                auth: auth
            });
        return response.data
    } catch (error) {
        console.error(error);
    }
}


async function getCollectionsPipeline(collection_id, type, name, version) {
    try {
        const response = await axios.get('https://api-gateway.sajari.com/v4/collections/' + collection_id + '/pipelines/' + type + '/' + name + '/' + version + "?view=FULL",
            {
                auth: auth
            });
        return response.data
    } catch (error) {
        console.error(error);
    }
}

async function createCollectionsPipeline(collection_id, type, name, version, pipelineData) {
    try {
        const response = await axios.post('https://api-gateway.sajari.com/v4/collections/' + collection_id + '/pipelines', {
            "type": type,
            "name": name,
            "version": version,
            "pre_steps": pipelineData.pre_steps,
            "post_steps": pipelineData.post_steps,
        },
            {
                auth: auth
            });
        return response.data;
    } catch (error) {
        console.error(error);
    }
}

async function create(baseCollectionName) {
    var toCollectionName = baseCollectionName + new Date().getTime();
    await createNewCollection(toCollectionName)

    console.log("==========================================")
    console.log("New Collection Name = " + toCollectionName)
    console.log("==========================================")

    return toCollectionName
}

async function copy(toCollectionName) {
    var fromCollectionName = process.env.COPY_FROM_COLLECTION_NAME;
    var fromPipelineRecordName = process.env.COPY_FROM_RECORD_NAME;
    var fromPipelineRecordVersion = process.env.COPY_FROM_PIPELINE_RECORD_VERSION;
    var fromPipelineQueryName = process.env.COPY_FROM_PIPELINE_QUERY_NAME;
    var fromPipelineQueryVersion = process.env.COPY_FROM_QUERY_VERSION;

    var devSchemaFields = await getCollectionsSchemaFields(fromCollectionName);

    await createCollectionsSchemaFields(toCollectionName, devSchemaFields);

    sleep.sleep(2);
    var recordPipeline = await getCollectionsPipeline(fromCollectionName, 'RECORD', fromPipelineRecordName, fromPipelineRecordVersion)
    await createCollectionsPipeline(toCollectionName, 'RECORD', fromPipelineRecordName, '1', recordPipeline)

    sleep.sleep(2);
    var queryPipeline = await getCollectionsPipeline(fromCollectionName, 'QUERY', fromPipelineQueryName, fromPipelineQueryVersion)
    await createCollectionsPipeline(toCollectionName, 'QUERY', fromPipelineQueryName, '1', queryPipeline)
}

async function main() {

    var baseCollectionName = process.env.BASE_NEW_COLLECTION_NAME;
    var toCollectionName = await create(baseCollectionName);

    sleep.sleep(10);

    await copy(toCollectionName);

}

main();


import * as crypto from 'crypto'
import * as qs from 'querystring'
import * as request from 'request'
import * as moment from "moment";
import * as csv from "fast-csv";
import * as fs from "fs";
import * as COS from 'cos-nodejs-sdk-v5';



const url = "https://vod.api.qcloud.com/v2/index.php";
let config = loadConfig();


function writeLog(logLeven, logStr, e, oriLogFunc) {
    let datetime = moment().format('YYYY-MM-DD HH:mm:ss');
    let date = moment().format('YYYY-MM-DD');
    var loc = e.stack.replace(/Error\n/).split(/\n/)[1].replace(/^\s+|\s+$/, "");
    oriLogFunc.call(console, `${datetime} | ${logLeven} | ${loc} | ${JSON.stringify(logStr)}`);
}




console.debug = (function (oriLogFunc) {
    return function (str) {
        try {
            throw new Error();
        } catch (e) {           
            if(config.DEBUG){
                writeLog('DEBUG', str, e, oriLogFunc);
            }
           
        }

    }
})(console.info);


//在这里可以修改配置
//config.param.output.bucket = "outputbucket1";
//config.param.output.dir = "video";
//config.param.mediaProcess.transcode.definition = [20];

//日志相关
//config.region = "ap-beijing";
//config.stat.enable = true;
//config.stat.path = log;
//config.stat.level = "EACH"

const TimeOut = 24 * 3600;

const RetryNum = 3;

function checkConfig(config) {

    if (!config) {
        return false;
    }

  
    if(config.stat.enable && !config.appId){
        return false;

    }

    if (!config.SecretId || !config.SecretKey ) {
        return false;
    }
}

//配置文件加载
export function loadConfig(configPath = __dirname + '/config.json') {
    return JSON.parse(fs.readFileSync(configPath, "utf8"));
}

//上传事件处理函数
export async function main_handler(event, context, callback, cusConfig) {
    if (cusConfig) {
        config = cusConfig;
    }

    if(!config.stat){
        config.stat = {};
    }

    try{
        if (config.type == "output") {
            //日志收集
            await collectorLog(config);
        } else {
            console.log("检测到COS事件");
            if (event && event.Records && Array.isArray(event.Records)) {
                for (let record of event.Records) {
    
                    if (config.param.output.bucket == record.cos.cosBucket.name) {
                        console.error("禁止输入bucket与输出bucket相同");
                        continue;
                    }
    
                    if (record.event && (record.event.eventName.indexOf("cos:ObjectCreated") == 0)) {
                        await handleInputRecord(record);
                    }
                }
            }
        }
    }catch(err){


        console.error(err);
    }
   
};




async function collectorEachLog(config) {



    let cos = new COS({
        "SecretId": config.SecretId,
        "SecretKey": config.SecretKey,
    });

    let eachLogCollector = new EachLogCollector(config);


    //拉取失败日志
    var errorParams = {
        Bucket: config.param.output.bucket + "-" + config.appId,
        Region: config.region,
        Prefix: `${config.stat.path}/vodlog/EACH/ERROR/`,                    /* 必须 */
    };



    let errResult: any = await getCosBucket(cos, errorParams);

    let errEachParamsList = [];
    if (errResult && Array.isArray(errResult.Contents)) {

        for (let content of errResult.Contents) {
            errEachParamsList.push({
                Bucket: errorParams.Bucket,          /* 必须 */
                Region: errorParams.Region,          /* 必须 */
                Key: content.Key,                    /* 必须 */
            });
        }

        try {
            await eachLogCollector.collectorErrorLog(errEachParamsList);
        } catch (err) {
            console.error(err);
        }

    }

    //拉取成功日志
    var successParams = {
        Bucket: config.param.output.bucket + "-" + config.appId,
        Region: config.region,
        Prefix: `${config.stat.path}/vodlog/EACH/SUCESS/`,                    /* 必须 */
    };


    let successResult: any = await getCosBucket(cos, successParams);



    let successEachParamsList = [];
    if (successResult && Array.isArray(successResult.Contents)) {

        for (let content of successResult.Contents) {
 
            successEachParamsList.push({
                Bucket: successParams.Bucket,          /* 必须 */
                Region: successParams.Region,          /* 必须 */
                Key: content.Key,                    /* 必须 */
            });
        }
        try {
            await eachLogCollector.collectorSucessLog(successEachParamsList);
        } catch (err) {
            console.error(err);
        }

    }
}



async function collectorLog(config) {
    let collector = collectorDayLog;
    switch (config.stat.level) {
        case "EACH":
            collector = collectorEachLog;
    }
    await collector(config);
}


//日志记录行定义
class LogItem {
    appId: string
    fileId: string
    uploadTime: string
    inputFile: string
    inputVideoUrl: string
    resCode: string
    resMessage: string
    vodTaskId: string
    outputFile: string
    status: string



    setValue(item) {
        if (!this.fileId) {
            return -1;
        }

        if (!item.fileId) {
            item.fileId = this.fileId
        }
        if (item.appId) this.appId = item.appId;
        if (item.uploadTime) this.uploadTime = item.uploadTime;
        if (item.inputFile) this.inputFile = item.inputFile;
        if (item.inputVideoUrl) this.inputVideoUrl = item.inputVideoUrl;
        if (item.resCode) this.resCode = item.resCode;
        if (item.resMessage) this.resMessage = item.resMessage;
        if (item.vodTaskId) this.vodTaskId = item.vodTaskId;
        if (item.status) this.status = item.status;
        if (item.outputFile) this.outputFile = item.outputFile;

    }


    static mergeLogItemWithFileId(item1: LogItem, item2: LogItem) {
        if (item1.fileId == item2.fileId) {
            if (item2.uploadTime) item1.uploadTime = item2.uploadTime;
            if (item2.inputFile) item1.inputFile = item2.inputFile;
            if (item2.inputVideoUrl) item1.inputVideoUrl = item2.inputVideoUrl;
            if (item2.resCode) item1.resCode = item2.resCode;
            if (item2.resMessage) item1.resMessage = item2.resMessage;
            if (item2.vodTaskId) item1.vodTaskId = item2.vodTaskId;
            if (item2.status) item1.status = item2.status;
            if (item2.outputFile) item1.outputFile = item2.outputFile;
        }

        return item1;
    }

    //合并远程日志与新日志,异步items2数量较少,远小于items1，这里不建立索引
    static mergeLogItemsWithFileId(items1, items2, force = true) {
        let items = [];
        const reg = /\.f[0-9]+$/;
        for (let i = 0; i < items2.length; i++) {
            let res = items2[i].fileId.match(reg);
            if (res) {
                items2[i]._fileIdType = 1;
            } else {
                items2[i]._fileIdType = 0;
            }
            for (let item of items1) {
                let tmpItem: any = {};
                Object.assign(tmpItem, items2[i]);
                if (!tmpItem._fileIdType) {
                    tmpItem.fileId = tmpItem.fileId + "." + getDefinitionDiffx(item.fileId);
                }
                if (tmpItem.fileId == item.fileId) {
                    items2[i]._FIND = true;
                }
                this.mergeLogItemWithFileId(item, tmpItem);
            }
            delete items2[i]._fileIdType;
        }

        if (force) {
            for (let item of items2) {
                if (!item._FIND) {
                    items.push(item);
                }
            }
        }

        return items.concat(items1);
    }

    equalWithFildId(item) {
        const reg = /\.f[0-9]+$/;

    }

    getObj() {
        return {
            fileId: this.fileId,
            uploadTime: this.uploadTime,
            inputFile: this.inputFile,
            inputVideoUrl: this.inputVideoUrl,
            resCode: this.resCode,
            resMessage: this.resMessage,
            vodTaskId: this.vodTaskId,
            status: this.status,
            outputFile: this.outputFile,
        }
    }

    getLogObj() {
        return {
            "文件ID": this.fileId,
            "文件上传时间": this.uploadTime,
            "转码源文件": this.inputFile,
            "源文件URL": this.inputVideoUrl,
            "转码请求返回码": this.resCode,
            "转码请求返回消息": this.resMessage,
            "vodTaskId": this.vodTaskId,
            "状态": this.status,
            "转码输出文件": this.outputFile,

        }
    }
}



function transcodeCheck(config) {

    try {
        if (!Array.isArray(config.param.mediaProcess.transcode.definition)) {
            return false;
        }

        if (!config.param.output.bucket) {
            return false;
        }

    } catch (err) {
        return false;
    }

    return true;

}


//视频转码
async function transcode(record) {

    try {
        let items = [];
        let fileIds = getTranscodeFileIds(record.cos.cosObject.key, config)

        let objectKey = new ObjectKey(record.cos.cosObject.key);
        for (let i = 0; i < fileIds.length; i++) {
            let value = new LogItem();
            value.fileId = fileIds[i];
            value.setValue({
                appId: record.cos.cosBucket.appid,
                uploadTime: moment().format("YYYY-MM-DD HH:mm:ss"),
                inputFile: record.cos.cosObject.key,
                inputVideoUrl: record.cos.cosObject.url,
            });
            if (config.param.output.dir) {
                value.outputFile = "/" + record.cos.cosBucket.appid + "/" + config.param.output.bucket + config.param.output.dir + objectKey.name;
            } else {
                let inputs = record.cos.cosObject.key.split("/");
                value.outputFile = "/" + record.cos.cosBucket.appid + "/" + config.param.output.bucket + "/" + inputs.slice(3, inputs.length - 1).join("/") + "/" + objectKey.name;
            }
            items.push(value);
        }

        let result = null;
        for (let i = 0; i < RetryNum; i++) {
            try {
                result = await requestTranscode(genQueryUrl(record));
            } catch (err) {
                console.error(err);
                console.error("请求失败，重试");
                continue
            }
            break;
        }

        if (result == null) {
            console.error("请求发出错误！！！");
            for (let item of items) {
                item.resMessage = "error"
            }
        } else {
            console.log("请求发出成功");
            console.log(result);
            for (let item of items) {
                item.resCode = result.code;
                item.resMessage = result.codeDesc;
                item.vodTaskId = result.vodTaskId
            }
        }
        //远程日志记录
        if (config.stat.enable) {
            try {
                await writeInputLog(config, items);
                //await requestTranscodeLog(config, items);
                console.log("写入远程日志成功");
            } catch (err) {

                console.error("写入远程日志失败");
            }

        }
    } catch (err) {
        console.error(err);
    }

}


async function handleInputRecord(record) {

    //只监控文件上传
    console.log("检测到文件上传");
    if (!isValid(record)) {
        return;
    }

    if (transcodeCheck(config)) {
        transcode(record);
    } else {
        console.log("转码参数错误");
    }
}

//处理完成转码记录
async function handleOutputRecord(record) {
    try {
        //只监控文件上传
        console.log("检测到转码成功文件上传");
        if (!isValid(record)) {
            return;
        }
        let item = {
            appId: record.cos.cosBucket.appid,
            fileId: getTranscdoeRequestKey(record.cos.cosObject.key),
            outputFile: record.cos.cosObject.key,
        }

        //远程日志记录
        if (config.stat.enable) {
            try {
                await transcodeResultLog(config, [item]);
                console.log("写入远程日志成功");
            } catch (err) {
                console.error(err);
                console.error("写入远程日志失败");
            }

        }
    } catch (err) {

        console.error(err);
    }

}



function getDefinitionDiffx(fileId) {
    if (!fileId) {
        return "";
    }

    const reg = /\.f[0-9]+$/;
    let res = fileId.match(reg);
    if (!res) {
        return "";
    }
    return fileId.substring(res['index'] + 1, fileId.length);
}





async function transcodeResultLog(config, items) {

    //生成文件名称
    let logHandler = null;
    switch (config.stat.level) {
        case "DAY":
            logHandler = new DayLogHander();
            break;
        case "EACH":
            logHandler = new EachLogHandler();
            break;
        default:
            logHandler = new DayLogHander();
            break;
    }
    await logHandler.writeOutputLog({ items, config });
}


async function requestTranscodeLog(config, items) {

    //生成文件名称
    let logHandler = null;
    switch (config.stat.level) {
        case "DAY":
            logHandler = new DayLogHander();
            break;
        case "EACH":
            logHandler = new EachLogHandler();
            break;
        default:
            logHandler = new DayLogHander();
            break;
    }
    await logHandler.writeInputLog({ items, config });

}


let videoExtends = new Set([".rmvb",".mp4", ".3pg", ".mov", ".m4v", ".avi", ".mkv", ".flv", ".vob", ".wmv", ".asf", ".asx", ".dat"]);

//过滤非视频文件
function isValid(record) {

    let objectKey = new ObjectKey(record.cos.cosObject.key);

    if (objectKey.extend && videoExtends.has(objectKey.extend)) {
        return true;
    }

    let contentType = record.cos.cosObject.meta["Content-Type"];
    if (contentType.indexOf("video") != 0) {
        console.log("忽略非视频文件");
        return false;
    }
    return true;
}

class ObjectKey {
    appId: string;
    bucket: string;
    path: string;
    name: string;
    extend: string;
    constructor(key: string) {
        let strs = key.split("/");
        this.appId = strs[1];
        this.bucket = strs[2];
        this.path = "/" + strs.slice(3, strs.length - 1).join("/");


        let longName = strs[strs.length - 1];
        let index = longName.lastIndexOf(".");
        if (index < 0) {
            this.name = longName;
            this.extend = "";
        }
        else {
            this.name = longName.substring(0, index);
            this.extend = longName.substring(index, longName.length);
        }
    }
}


//请求视频转码
function requestTranscode(queryUrl) {
    return new Promise(function (resolve, reject) {
        let proxy = "";
        if(config.PROXY){
            proxy = config.PROXY
        }
        request({ url: queryUrl, timeout: 5000 ,proxy}, function (error, response, body) {
            //记录日志
            if (error) {
                reject(error);
            } else {
                try {
                    body = JSON.parse(body);
                    if (body.code == 0) {
                        resolve(body);
                    } else {
                        reject(body);
                    }
                } catch (error) {
                    reject(error);
                }
            }
        })
    });
}

function getTaskInfo(vodTaskId) {
    return new Promise(function (resolve, reject) {
        let params = {};
        params['Action'] = "GetTaskInfo";
        params['vodTaskId'] = vodTaskId;
        params['Region'] = "gz";
        params['Timestamp'] = Math.round(Date.now() / 1000);
        params['Nonce'] = Math.round(Math.random() * 65535);
        params['SecretId'] = config.SecretId;
        let queryUrl = genUrl(params, config.SecretKey);
        request({ url: queryUrl, timeout: 5000 }, function (error, response, body) {
            //记录日志
            if (error) {
                reject(error);
            } else {
                try {
                    body = JSON.parse(body);
                    if (body.code == 0) {
                        resolve(body);
                    } else {
                        reject(body);
                    }
                } catch (error) {
                    reject(error);
                }
            }
        })
    })
}




async function writeInputLog(config, items) {

    if (items.length == 0) {
        return;
    }
    var cos = new COS({
        SecretId: config.SecretId,
        SecretKey: config.SecretKey,
    });
    let item = items[0];


    for (let item of items) {
        //如果成功
        let logKey = "";
        if (item.vodTaskId) {
            logKey = `${config.stat.path}/vodlog/${config.stat.level}/SUCESS/${item.fileId}/${item.vodTaskId}.csv`;
        } else {
            logKey = `${config.stat.path}/vodlog/${config.stat.level}/ERROR/${item.fileId}/${crypto.createHash('md5').update(new Date() + "").digest("hex")}.csv`;
        }

        let params = {
            Bucket: config.param.output.bucket + "-" + item.appId,
            Region: config.region,
            Key: logKey,
        }
        await writeRemoteCsv([item], cos, params);
    }
}


/*************************************LogCollector ********************************/

abstract class LogCollector {


    cos: any
    constructor(config) {
        this.cos = new COS({
            "SecretId": config.SecretId,
            "SecretKey": config.SecretKey,
        });
    }

    abstract async   collectorErrorLog(paramsList: Array<any>);

    abstract async collectorSucessLog(paramsList: Array<any>);
}


class EachLogCollector extends LogCollector {

    async collectorErrorLog(paramsList: Array<any>) {

        if (paramsList.length == 0) {
            return;
        }
        let params = paramsList[0];


        let deleteKeys = [];

        try {
            for (let params of paramsList) {

                //reduce 错误日志
                let errorItems: any = await getRemoteLogItems(this.cos, params);

                if (!errorItems && errorItems.length == 0) {
                    continue;
                }

                let errorItem = errorItems[0];

                let remoteParam: any = {};
                remoteParam = Object.assign(remoteParam, params);
                remoteParam.Key = `${config.stat.path}/${errorItem.fileId}.csv`;
                await writeRemoteCsv([errorItem], this.cos, remoteParam)

            }

            try {
                let deleteParams: any = {};
                deleteParams = Object.assign(deleteParams, paramsList[0]);
                deleteParams.Objects = [];
                for (let params of paramsList) {
                    deleteParams.Objects.push({ Key: params.Key });
                }

                await deleteMultipleCosObject(this.cos, deleteParams);
            } catch (err) {
                console.error(err);
            }

        } catch (err) {
            throw err
        }
    }

    async collectorSucessLog(paramsList: Array<any>) {


        if (paramsList.length == 0) {
            return;
        }

        let params = paramsList[0];
        let deleteParamsList = [];
        let remoteItems = {};
        try {
            for (let params of paramsList) {
                

                let sucessItems: any = await getRemoteLogItems(this.cos, params);


                if (!sucessItems || sucessItems.length == 0) {
                    continue;
                }

                let sucessItem = sucessItems[0];

                let orgStatus = sucessItem.status;
                //查看是否完成成功
                try {

                    //需要更新到日志
                    let needMerge = false;
                    let needDelete = false;

                    if (!sucessItem.status) {
                        needMerge = true;
                    }

                    let inputs = sucessItem.outputFile.split("/");
                    let Prefix = inputs.slice(3, inputs.length).join("/");

                    var outputParams = {
                        Bucket: config.param.output.bucket + "-" + config.appId,
                        Region: config.region,
                        Prefix
                    };


                    let outputFile: any = await getCosBucket(this.cos, outputParams);



                    if (outputFile && Array.isArray(outputFile.Contents) && outputFile.Contents.length > 0) {
                        sucessItem.status = "FINISH";
                        needMerge = true;
                        needDelete = true;

                    } else {
                        if ((moment().unix() - moment(sucessItem.uploadTime).unix()) > TimeOut) {
                            sucessItem.status = "TimeOut";
                            needMerge = true;
                            needDelete = true;
                        }
                        sucessItem.status = "PROCESSING";
                    }

                    if (needMerge) {
                        let remoteParam: any = {};
                        remoteParam = Object.assign(remoteParam, params);
                        remoteParam.Key = `${config.stat.path}/${sucessItem.fileId}.csv`;

                        await writeRemoteCsv([sucessItem], this.cos, remoteParam)
                    }


                    if (needDelete) {
                        deleteParamsList.push(params);
                    } else if (orgStatus != sucessItem.status) {
                        await writeRemoteCsv([sucessItem], this.cos, params);
                    }
                } catch (err) {
                    console.error(err);
                }
            }


            try {
                let deleteParams: any = {};
                deleteParams = Object.assign(deleteParams, deleteParamsList[0]);
                deleteParams.Objects = [];
                for (let params of deleteParamsList) {
                    deleteParams.Objects.push({ Key: params.Key });
                }

                if(deleteParams.Objects.length > 0){
                    await deleteMultipleCosObject(this.cos, deleteParams);
                }
                
            } catch (err) {
                console.error(err);
            }
        } catch (err) {

            throw (err);

        }

    }

}




class DayLogCollector extends LogCollector {

    async collectorErrorLog(paramsList: Array<any>) {
        if (paramsList.length == 0) {
            return;
        }
        let params = paramsList[0];
        let remoteItems = {};
        try {
            for (let params of paramsList) {
                //reduce 错误日志
                let errorItems: any = await getRemoteLogItems(this.cos, params);
                if (!errorItems && errorItems.length == 0) {
                    continue;
                }

                let errorItem = errorItems[0];
                let Day = moment(errorItem.uploadTime).format("YYYY-MM-DD");

                if (!remoteItems[Day]) {
                    let remoteCosParams = {
                        Bucket: params.Bucket,
                        Region: params.Region,
                        Key: `${config.stat.path}/${Day}.csv`

                    }
                    try {
                        remoteItems[Day] = await getRemoteLogItems(this.cos, remoteCosParams);
                    } catch (err) {
                        remoteItems[Day] = [];
                    }

                }
                remoteItems[Day] = LogItem.mergeLogItemsWithFileId(remoteItems[Day], errorItems);

            }


            for (let dayKey in remoteItems) {
                let remoteItem = remoteItems[dayKey];
                let remoteCosParams = {
                    Bucket: params.Bucket,
                    Region: params.Region,
                    Key: `${config.stat.path}/${dayKey}.csv`
                }
                await writeRemoteCsv(remoteItem, this.cos, remoteCosParams);
            }
            try {
                let deleteParams: any = {};
                deleteParams = Object.assign(deleteParams, paramsList[0]);
                deleteParams.Objects = [];
                for (let params of paramsList) {
                    deleteParams.Objects.push({ Key: params.Key });
                }

                await deleteMultipleCosObject(this.cos, deleteParams);
            } catch (err) {
                console.error(err);
            }

        } catch (err) {
            throw err
        }
    }

    async collectorSucessLog(paramsList: Array<any>) {

        if (paramsList.length == 0) {
            return;
        }

        let params = paramsList[0];
        let deleteParamsList = [];
        let remoteItems = {};
        try {
            for (let params of paramsList) {
                //reduce 错误日志



                let sucessItems: any = await getRemoteLogItems(this.cos, params);
                if (!sucessItems || sucessItems.length == 0) {
                    continue;
                }

                let sucessItem = sucessItems[0];

                let orgStatus = sucessItem.status;
                //查看是否完成成功
                try {

                    //查看文件哟就没有生成



                    //需要更新到日志
                    let needMerge = false;
                    let needDelete = false;

                    if (!sucessItem.status) {
                        needMerge = true;
                    }



                    let inputs = sucessItem.outputFile.split("/");
                    let Prefix = inputs.slice(3, inputs.length).join("/");

                    var outputParams = {
                        Bucket: config.param.output.bucket + "-" + config.appId,
                        Region: config.region,
                        Prefix
                    };


                    let outputFile: any = await getCosBucket(this.cos, outputParams);

                    if (outputFile && Array.isArray(outputFile.Contents) && outputFile.Contents.length > 0) {
                        sucessItem.status = "FINISH";
                        needMerge = true;
                        needDelete = true;

                    } else {
                        if ((moment().unix() - moment(sucessItem.uploadTime).unix()) > TimeOut) {
                            sucessItem.status = "TimeOut";
                            needMerge = true;
                            needDelete = true;
                        }
                        sucessItem.status = "PROCESSING";
                    }


                    if (needMerge) {
                        let Day = moment(sucessItem.uploadTime).format("YYYY-MM-DD");
                        if (!remoteItems[Day]) {
                            let remoteCosParams = {
                                Bucket: params.Bucket,
                                Region: params.Region,
                                Key: `${config.stat.path}/${Day}.csv`
                            }
                            try {
                                remoteItems[Day] = await getRemoteLogItems(this.cos, remoteCosParams);
                            } catch (err) {
                                remoteItems[Day] = [];
                            }
                        }
                        remoteItems[Day] = LogItem.mergeLogItemsWithFileId(remoteItems[Day], [sucessItem]);
                    }

                    if (needDelete) {
                        deleteParamsList.push(params);
                    } else if (orgStatus != sucessItem.status) {
                        await writeRemoteCsv([sucessItem], this.cos, params);
                    }
                } catch (err) {
                    console.error(err);
                }
            }

            for (let dayKey in remoteItems) {
                let remoteItem = remoteItems[dayKey];
                let remoteCosParams = {
                    Bucket: params.Bucket,
                    Region: params.Region,
                    Key: `${config.stat.path}/${dayKey}.csv`
                }
                await writeRemoteCsv(remoteItem, this.cos, remoteCosParams);
            }

            try {
                let deleteParams: any = {};
                deleteParams = Object.assign(deleteParams, deleteParamsList[0]);
                deleteParams.Objects = [];
                for (let params of deleteParamsList) {
                    deleteParams.Objects.push({ Key: params.Key });
                }
                await deleteMultipleCosObject(this.cos, deleteParams);
            } catch (err) {
                console.error(err);
            }
        } catch (err) {

            throw (err);

        }
    }
}



async function collectorDayLog(config) {
    let cos = new COS({
        "SecretId": config.SecretId,
        "SecretKey": config.SecretKey,
    });

    let dayLogCollector = new DayLogCollector(config);


    //拉取失败日志
    var errorParams = {
        Bucket: config.param.output.bucket + "-" + config.appId,
        Region: config.region,
        Prefix: `${config.stat.path}/vodlog/DAY/ERROR/`,                    /* 必须 */
    };


    let errResult: any = await getCosBucket(cos, errorParams);
    let errDayParamsList = [];
    if (errResult && Array.isArray(errResult.Contents)) {

        for (let content of errResult.Contents) {
            errDayParamsList.push({
                Bucket: errorParams.Bucket,          /* 必须 */
                Region: errorParams.Region,          /* 必须 */
                Key: content.Key,                    /* 必须 */
            });
        }
        try {
            await dayLogCollector.collectorErrorLog(errDayParamsList);
        } catch (err) {
            console.error(err);
        }

    }


    //拉取成功日志
    var successParams = {
        Bucket: config.param.output.bucket + "-" + config.appId,
        Region: config.region,
        Prefix: `${config.stat.path}/vodlog/DAY/SUCESS/`,                    /* 必须 */
    };


    let successResult: any = await getCosBucket(cos, successParams);
    let successDayParamsList = [];
    if (successResult && Array.isArray(successResult.Contents)) {

        for (let content of successResult.Contents) {
            successDayParamsList.push({
                Bucket: successParams.Bucket,          /* 必须 */
                Region: successParams.Region,          /* 必须 */
                Key: content.Key,                    /* 必须 */
            });
        }
        try {
            await dayLogCollector.collectorSucessLog(successDayParamsList);
        } catch (err) {
            console.error(err);
        }

    }

}




/******************************** LogHandler *************************************/

abstract class LogHandler {
    //写入请求转码日志
    abstract async writeInputLog({ items, config });

    //写入转码文件上传日志
    abstract async writeOutputLog({ items, config });
}



//按请求记录日志
class EachLogHandler extends LogHandler {
    async writeInputLog({ items, config }) {
        //直接覆盖源文件
        if (!items || items.length == 0) {
            return;
        }
        var cos = new COS({
            SecretId: config.SecretId,
            SecretKey: config.SecretKey,
        });

        let item = items[0];
        let params = {
            Bucket: config.param.output.bucket + "-" + item.appId,
            Region: config.region,
            Key: getTranscdoeRequestKey(item.inputFile, config) + ".csv",

        }
        await writeRemoteCsv(items, cos, params);
    }
    async writeOutputLog({ items, config }) {

        //直接覆盖源文件
        if (!items || items.length == 0) {
            return;
        }
        var cos = new COS({
            SecretId: config.SecretId,
            SecretKey: config.SecretKey,
        });

        let item = items[0];

        let key = getTranscdoeRequestKey(item.outputFile);


        //无法判断文件名本来就含有.fxx,这里假设用户永远不上上传后缀名带有fxx的视频
        var reg = /\.f[0-9]+$/;
        var res = key.match(reg);
        if (res) {
            key = key.substring(0, res['index'])
        }

        try {
            let params = {
                Bucket: config.param.output.bucket + "-" + item.appId,
                Region: config.region,
                Key: key + ".csv",
            }
            let remoteItems: any = await getRemoteLogItems(cos, params);
            remoteItems = await LogItem.mergeLogItemsWithFileId(remoteItems, items, false)
            await writeRemoteCsv(remoteItems, cos, params);

        } catch (err) {
            console.error(err);

        }

    }
}





//按天记录日志
class DayLogHander extends LogHandler {
    async writeInputLog({ items, config }) {
        if (!items || items.length == 0) {
            return;
        }
        let logKey = moment().format("YYYY-MM-DD") + ".csv";
        var cos = new COS({
            SecretId: config.SecretId,
            SecretKey: config.SecretKey,
        });
        let item = items[0];
        let params = {
            Bucket: config.param.output.bucket + "-" + item.appId,
            Region: config.region,
            Key: logKey,
        }

        //1 获取远程日志
        let remoteItems = null;
        try {
            remoteItems = await getRemoteLogItems(cos, params);
        } catch (err) {

        }
        if (!remoteItems) {
            remoteItems = [];
        }


        try {
            //2 更新日志内容
            items = LogItem.mergeLogItemsWithFileId(remoteItems, items)
            //3 写入远程日志
            await writeRemoteCsv(items, cos, params);

        } catch (err) {
            console.error(err)
        }
    }
    async writeOutputLog({ items, config }) {

        //拉取今天的更新
        if (!items || items.length == 0) {
            return;
        }

        var cos = new COS({
            SecretId: config.SecretId,
            SecretKey: config.SecretKey,
        });
        let item = items[0];
        let todayLogKey = moment().format("YYYY-MM-DD") + ".csv";
        let params = {
            Bucket: config.param.output.bucket + "-" + item.appId,
            Region: config.region,
            Key: todayLogKey,
        }

        //1 获取远程日志
        let todayRemoteItems = null;
        try {
            todayRemoteItems = await getRemoteLogItems(cos, params);
        } catch (err) {
        }
        if (!todayRemoteItems) {
            todayRemoteItems = [];
        }

        let length = todayRemoteItems.length;
        todayRemoteItems = LogItem.mergeLogItemsWithFileId(todayRemoteItems, items, false);
        if (items[0]._FIND) {
            await writeRemoteCsv(todayRemoteItems, cos, params);
            return;
        }


        //更新失败则拉取昨天的更新
        let yesterdayLogKey = moment().subtract(1, 'days').format("YYYY-MM-DD") + ".csv";
        params = {
            Bucket: config.param.output.bucket + "-" + item.appId,
            Region: config.region,
            Key: todayLogKey,
        }
        let yesterdayRemoteItems = null;
        try {
            yesterdayRemoteItems = await getRemoteLogItems(cos, params);
        } catch (err) {
        }
        if (!yesterdayRemoteItems) {
            return;
        }
        length = todayRemoteItems.length();
        yesterdayRemoteItems = LogItem.mergeLogItemsWithFileId(yesterdayRemoteItems, items, false);
        if (items[0]._FIND) {
            await writeRemoteCsv(yesterdayRemoteItems, cos, params);
            return;
        }
    }
}




/***************************************************utils ****************/


function normlizePath(path) {


    if (!path) {
        return "";
    }

    if(path==="/"){
        return path;
    }

    let splits = path.split("/").filter(function (str) {
        return str != "";
    });

    return splits.join("/");
}


function getTranscdoeRequestKey(filePath, config = null) {
    let fileIds = [];
    let file = new ObjectKey(filePath);

    let strs = [];
    strs.push(file.appId);
    if (config && config.param.output.dir) {
        strs = strs.concat(config.param.output.dir.split("/"))
    } else {
        strs = strs.concat(file.path.split("/"))
    }
    strs.push(file.name);
    return strs.join("_")
}


//获取所有文件ID
function getTranscodeFileIds(filePath, config) {
    let fileIds = []
    let requestKey = getTranscdoeRequestKey(filePath, config)
    for (let definition of config.param.mediaProcess.transcode.definition) {
        fileIds.push(requestKey + ".f" + definition)
    }
    return fileIds;
}


function genQueryUrl(record) {
    let params = {};
    params['input.bucket'] = record.cos.cosBucket.name;
    let inputs = record.cos.cosObject.key.split("/");
    params['input.path'] = "/" + inputs.slice(3).join("/");
    params['output.bucket'] = config.param.output.bucket;
    if (config.param.output.dir) {
        params['output.dir'] = config.param.output.dir;
    } else {
        params['output.dir'] = "/" + inputs.slice(3, inputs.length - 1).join("/") + "/";
    }
    params['mediaProcess.transcode.definition.0'] = config.param.mediaProcess.transcode.definition[0];
    params['Action'] = "ProcessCosMedia";
    params['Region'] = record.cos.cosBucket.region;
    params['Timestamp'] = Math.round(Date.now() / 1000);
    params['Nonce'] = Math.round(Math.random() * 65535);
    params['SecretId'] = config.SecretId;
    console.log("生成参数");
    console.log(params);
    let queryUrl = genUrl(params, config.SecretKey);
    console.log("生成请求");
    console.log(queryUrl);
    return queryUrl;
}


function genUrl(params, secretKey) {
    var keys = Object.keys(params);
    keys.sort();
    let qstr = "";
    keys.forEach(function (key) {
        var val = params[key]
        if (val === undefined || val === null || (typeof val === 'number' && isNaN(val))) {
            val = ''
        }
        qstr += '&' + (key.indexOf('_') ? key.replace(/_/g, '.') : key) + '=' + val
    })
    qstr = qstr.slice(1);
    let signature = sign("GET" + "vod.api.qcloud.com/v2/index.php" + '?' + qstr, secretKey);
    params.Signature = signature
    return url + "?" + qs.stringify(params);
}


function sign(str, secretKey) {
    var hmac = crypto.createHmac('sha1', secretKey || '')
    return hmac.update(new Buffer(str, 'utf8')).digest('base64')
}



/****************************cos async/await  */


function getCosBucket(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.getBucket(params, function (err, data) {
            if (err) {
                reject(err);
            } else {

                resolve(data);
            }
        });
    });

}




async function isCosObjectExist(cos, params) {
    try {
        let result = await headCosObject(cos, params);
        return true;
    } catch (err) {
        return false;
    }
}


function headCosObject(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.headObject(params, function (err, data) {
            if (err) {
                reject(err)
            } else {
                if (data.statusCode == 200) {
                    resolve(data.headers);
                } else {
                    reject({ message: "not found" });
                }

            }
        });
    });
}

function deleteCosObject(cos, params) {

    return new Promise(function (resolve, reject) {
        cos.deleteObject(params, function (err, data) {
            if (err) {
                reject(err)
            } else {
                resolve(data);
            }
        });
    });
}


function deleteMultipleCosObject(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.deleteMultipleObject(params, function (err, data) {
            if (err) {
                reject(err)
            } else {
                resolve(data);
            }
        });
    });
}

function getCosObject(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.getObject(params, function (err, data) {
            if (err) {
                reject(err)
            } else {
                resolve(data.Body);
            }
        });
    });
}




function putCosObject(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.putObject(params, function (err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
}


/***************remote csv tools */


//写入远程Csv文件
function writeRemoteCsv(items, cos, params) {
    return new Promise(function (resolve, reject) {
        let logFile = "/tmp/" + crypto.createHash('md5').update(new Date() + "").digest("hex") + ".csv";
        let csvStream = csv.createWriteStream({ headers: true });
        let writableStream = fs.createWriteStream(logFile);
        writableStream.on("finish", async function () {
            params['Body'] = fs.createReadStream(logFile);
            params["ContentLength"] = fs.statSync(logFile).size;
            let result = null;
            for (let i = 0; i < RetryNum; i++) {
                try {

                    result = await putCosObject(cos, params);
                } catch (err) {
                    console.error(err);
                    continue;
                }
                break;
            }
            if (result == null) {
                reject({ message: "远程日志写入失败" });
            } else {
                resolve({ message: "远程日志写入成功" });
            }

            try {
                fs.unlinkSync(logFile);
            } catch (err) {

            }
        });

        writableStream.on("error", function (err) {
            reject({ message: "写入 csv 文件失败" });
        });
        csvStream.pipe(writableStream);
        for (let item of items) {
            if (typeof item.getObj == "function") {
                csvStream.write(item.getObj());
            } else {
                csvStream.write(item);
            }

        }
        csvStream.end();
    });
}


//获取远程日志文件项目
function getRemoteLogItems(cos, params) {
    return new Promise(async function (resolve, reject) {

        let items = [];
        let remoteData = null;
        for (let i = 0; i < RetryNum; i++) {
            try {
                let exist = await isCosObjectExist(cos, params)
                if (exist) {
                    remoteData = await getCosObject(cos, params);
                }
            } catch (err) {
                continue;
            }
            break;
        }
        if (remoteData == null) {
            reject({ message: "读取远程日志失败" });
        }
        csv.fromString(remoteData, { headers: true })
            .on("data", function (data) {
                items.push(data);
            })
            .on("end", function () {
                resolve(items);
            });
    });
}


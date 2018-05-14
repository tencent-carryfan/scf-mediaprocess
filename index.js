"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = Object.setPrototypeOf ||
        ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
        function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = y[op[0] & 2 ? "return" : op[0] ? "throw" : "next"]) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [0, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var crypto = require("crypto");
var qs = require("querystring");
var request = require("request");
var moment = require("moment");
var csv = require("fast-csv");
var fs = require("fs");
var COS = require("cos-nodejs-sdk-v5");
var url = "https://vod.api.qcloud.com/v2/index.php";
var config = loadConfig();
function writeLog(logLeven, logStr, e, oriLogFunc) {
    var datetime = moment().format('YYYY-MM-DD HH:mm:ss');
    var date = moment().format('YYYY-MM-DD');
    var loc = e.stack.replace(/Error\n/).split(/\n/)[1].replace(/^\s+|\s+$/, "");
    oriLogFunc.call(console, datetime + " | " + logLeven + " | " + loc + " | " + JSON.stringify(logStr));
}
console.debug = (function (oriLogFunc) {
    return function (str) {
        try {
            throw new Error();
        }
        catch (e) {
            if (config.DEBUG) {
                writeLog('DEBUG', str, e, oriLogFunc);
            }
        }
    };
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
var TimeOut = 24 * 3600;
var RetryNum = 3;
function checkConfig(config) {
    if (!config) {
        return false;
    }
    if (config.stat.enable && !config.appId) {
        return false;
    }
    if (!config.SecretId || !config.SecretKey) {
        return false;
    }
}
//配置文件加载
function loadConfig(configPath) {
    if (configPath === void 0) { configPath = __dirname + '/config.json'; }
    return JSON.parse(fs.readFileSync(configPath, "utf8"));
}
exports.loadConfig = loadConfig;
//上传事件处理函数
function main_handler(event, context, callback, cusConfig) {
    return __awaiter(this, void 0, void 0, function () {
        var _i, _a, record, err_1;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    if (cusConfig) {
                        config = cusConfig;
                    }
                    if (!config.stat) {
                        config.stat = {};
                    }
                    _b.label = 1;
                case 1:
                    _b.trys.push([1, 8, , 9]);
                    if (!(config.type == "output")) return [3 /*break*/, 3];
                    //日志收集
                    return [4 /*yield*/, collectorLog(config)];
                case 2:
                    //日志收集
                    _b.sent();
                    return [3 /*break*/, 7];
                case 3:
                    console.log("检测到COS事件");
                    if (!(event && event.Records && Array.isArray(event.Records))) return [3 /*break*/, 7];
                    _i = 0, _a = event.Records;
                    _b.label = 4;
                case 4:
                    if (!(_i < _a.length)) return [3 /*break*/, 7];
                    record = _a[_i];
                    if (config.param.output.bucket == record.cos.cosBucket.name) {
                        console.error("禁止输入bucket与输出bucket相同");
                        return [3 /*break*/, 6];
                    }
                    if (!(record.event && (record.event.eventName.indexOf("cos:ObjectCreated") == 0))) return [3 /*break*/, 6];
                    return [4 /*yield*/, handleInputRecord(record)];
                case 5:
                    _b.sent();
                    _b.label = 6;
                case 6:
                    _i++;
                    return [3 /*break*/, 4];
                case 7: return [3 /*break*/, 9];
                case 8:
                    err_1 = _b.sent();
                    console.error(err_1);
                    return [3 /*break*/, 9];
                case 9: return [2 /*return*/];
            }
        });
    });
}
exports.main_handler = main_handler;
;
function collectorEachLog(config) {
    return __awaiter(this, void 0, void 0, function () {
        var cos, eachLogCollector, errorParams, errResult, errEachParamsList, _i, _a, content, err_2, successParams, successResult, successEachParamsList, _b, _c, content, err_3;
        return __generator(this, function (_d) {
            switch (_d.label) {
                case 0:
                    cos = new COS({
                        "SecretId": config.SecretId,
                        "SecretKey": config.SecretKey,
                    });
                    eachLogCollector = new EachLogCollector(config);
                    errorParams = {
                        Bucket: config.param.output.bucket + "-" + config.appId,
                        Region: config.region,
                        Prefix: config.stat.path + "/vodlog/EACH/ERROR/",
                    };
                    return [4 /*yield*/, getCosBucket(cos, errorParams)];
                case 1:
                    errResult = _d.sent();
                    errEachParamsList = [];
                    if (!(errResult && Array.isArray(errResult.Contents))) return [3 /*break*/, 5];
                    for (_i = 0, _a = errResult.Contents; _i < _a.length; _i++) {
                        content = _a[_i];
                        errEachParamsList.push({
                            Bucket: errorParams.Bucket,
                            Region: errorParams.Region,
                            Key: content.Key,
                        });
                    }
                    _d.label = 2;
                case 2:
                    _d.trys.push([2, 4, , 5]);
                    return [4 /*yield*/, eachLogCollector.collectorErrorLog(errEachParamsList)];
                case 3:
                    _d.sent();
                    return [3 /*break*/, 5];
                case 4:
                    err_2 = _d.sent();
                    console.error(err_2);
                    return [3 /*break*/, 5];
                case 5:
                    successParams = {
                        Bucket: config.param.output.bucket + "-" + config.appId,
                        Region: config.region,
                        Prefix: config.stat.path + "/vodlog/EACH/SUCESS/",
                    };
                    return [4 /*yield*/, getCosBucket(cos, successParams)];
                case 6:
                    successResult = _d.sent();
                    successEachParamsList = [];
                    if (!(successResult && Array.isArray(successResult.Contents))) return [3 /*break*/, 10];
                    for (_b = 0, _c = successResult.Contents; _b < _c.length; _b++) {
                        content = _c[_b];
                        successEachParamsList.push({
                            Bucket: successParams.Bucket,
                            Region: successParams.Region,
                            Key: content.Key,
                        });
                    }
                    _d.label = 7;
                case 7:
                    _d.trys.push([7, 9, , 10]);
                    return [4 /*yield*/, eachLogCollector.collectorSucessLog(successEachParamsList)];
                case 8:
                    _d.sent();
                    return [3 /*break*/, 10];
                case 9:
                    err_3 = _d.sent();
                    console.error(err_3);
                    return [3 /*break*/, 10];
                case 10: return [2 /*return*/];
            }
        });
    });
}
function collectorLog(config) {
    return __awaiter(this, void 0, void 0, function () {
        var collector;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    collector = collectorDayLog;
                    switch (config.stat.level) {
                        case "EACH":
                            collector = collectorEachLog;
                    }
                    return [4 /*yield*/, collector(config)];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
//日志记录行定义
var LogItem = /** @class */ (function () {
    function LogItem() {
    }
    LogItem.prototype.setValue = function (item) {
        if (!this.fileId) {
            return -1;
        }
        if (!item.fileId) {
            item.fileId = this.fileId;
        }
        if (item.appId)
            this.appId = item.appId;
        if (item.uploadTime)
            this.uploadTime = item.uploadTime;
        if (item.inputFile)
            this.inputFile = item.inputFile;
        if (item.inputVideoUrl)
            this.inputVideoUrl = item.inputVideoUrl;
        if (item.resCode)
            this.resCode = item.resCode;
        if (item.resMessage)
            this.resMessage = item.resMessage;
        if (item.vodTaskId)
            this.vodTaskId = item.vodTaskId;
        if (item.status)
            this.status = item.status;
        if (item.outputFile)
            this.outputFile = item.outputFile;
    };
    LogItem.mergeLogItemWithFileId = function (item1, item2) {
        if (item1.fileId == item2.fileId) {
            if (item2.uploadTime)
                item1.uploadTime = item2.uploadTime;
            if (item2.inputFile)
                item1.inputFile = item2.inputFile;
            if (item2.inputVideoUrl)
                item1.inputVideoUrl = item2.inputVideoUrl;
            if (item2.resCode)
                item1.resCode = item2.resCode;
            if (item2.resMessage)
                item1.resMessage = item2.resMessage;
            if (item2.vodTaskId)
                item1.vodTaskId = item2.vodTaskId;
            if (item2.status)
                item1.status = item2.status;
            if (item2.outputFile)
                item1.outputFile = item2.outputFile;
        }
        return item1;
    };
    //合并远程日志与新日志,异步items2数量较少,远小于items1，这里不建立索引
    LogItem.mergeLogItemsWithFileId = function (items1, items2, force) {
        if (force === void 0) { force = true; }
        var items = [];
        var reg = /\.f[0-9]+$/;
        for (var i = 0; i < items2.length; i++) {
            var res = items2[i].fileId.match(reg);
            if (res) {
                items2[i]._fileIdType = 1;
            }
            else {
                items2[i]._fileIdType = 0;
            }
            for (var _i = 0, items1_1 = items1; _i < items1_1.length; _i++) {
                var item = items1_1[_i];
                var tmpItem = {};
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
            for (var _a = 0, items2_1 = items2; _a < items2_1.length; _a++) {
                var item = items2_1[_a];
                if (!item._FIND) {
                    items.push(item);
                }
            }
        }
        return items.concat(items1);
    };
    LogItem.prototype.equalWithFildId = function (item) {
        var reg = /\.f[0-9]+$/;
    };
    LogItem.prototype.getObj = function () {
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
        };
    };
    LogItem.prototype.getLogObj = function () {
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
        };
    };
    return LogItem;
}());
function transcodeCheck(config) {
    try {
        if (!Array.isArray(config.param.mediaProcess.transcode.definition)) {
            return false;
        }
        if (!config.param.output.bucket) {
            return false;
        }
    }
    catch (err) {
        return false;
    }
    return true;
}
//视频转码
function transcode(record) {
    return __awaiter(this, void 0, void 0, function () {
        var items, fileIds, objectKey, i, value, inputs, result, i, err_4, _i, items_1, item, _a, items_2, item, err_5, err_6;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    _b.trys.push([0, 12, , 13]);
                    items = [];
                    fileIds = getTranscodeFileIds(record.cos.cosObject.key, config);
                    objectKey = new ObjectKey(record.cos.cosObject.key);
                    for (i = 0; i < fileIds.length; i++) {
                        value = new LogItem();
                        value.fileId = fileIds[i];
                        value.setValue({
                            appId: record.cos.cosBucket.appid,
                            uploadTime: moment().format("YYYY-MM-DD HH:mm:ss"),
                            inputFile: record.cos.cosObject.key,
                            inputVideoUrl: record.cos.cosObject.url,
                        });
                        if (config.param.output.dir) {
                            value.outputFile = "/" + record.cos.cosBucket.appid + "/" + config.param.output.bucket + config.param.output.dir + objectKey.name;
                        }
                        else {
                            inputs = record.cos.cosObject.key.split("/");
                            value.outputFile = "/" + record.cos.cosBucket.appid + "/" + config.param.output.bucket + "/" + inputs.slice(3, inputs.length - 1).join("/") + "/" + objectKey.name;
                        }
                        items.push(value);
                    }
                    result = null;
                    i = 0;
                    _b.label = 1;
                case 1:
                    if (!(i < RetryNum)) return [3 /*break*/, 7];
                    _b.label = 2;
                case 2:
                    _b.trys.push([2, 4, , 5]);
                    return [4 /*yield*/, requestTranscode(genQueryUrl(record))];
                case 3:
                    result = _b.sent();
                    return [3 /*break*/, 5];
                case 4:
                    err_4 = _b.sent();
                    console.error(err_4);
                    console.error("请求失败，重试");
                    return [3 /*break*/, 6];
                case 5: return [3 /*break*/, 7];
                case 6:
                    i++;
                    return [3 /*break*/, 1];
                case 7:
                    if (result == null) {
                        console.error("请求发出错误！！！");
                        for (_i = 0, items_1 = items; _i < items_1.length; _i++) {
                            item = items_1[_i];
                            item.resMessage = "error";
                        }
                    }
                    else {
                        console.log("请求发出成功");
                        console.log(result);
                        for (_a = 0, items_2 = items; _a < items_2.length; _a++) {
                            item = items_2[_a];
                            item.resCode = result.code;
                            item.resMessage = result.codeDesc;
                            item.vodTaskId = result.vodTaskId;
                        }
                    }
                    if (!config.stat.enable) return [3 /*break*/, 11];
                    _b.label = 8;
                case 8:
                    _b.trys.push([8, 10, , 11]);
                    return [4 /*yield*/, writeInputLog(config, items)];
                case 9:
                    _b.sent();
                    //await requestTranscodeLog(config, items);
                    console.log("写入远程日志成功");
                    return [3 /*break*/, 11];
                case 10:
                    err_5 = _b.sent();
                    console.error("写入远程日志失败");
                    return [3 /*break*/, 11];
                case 11: return [3 /*break*/, 13];
                case 12:
                    err_6 = _b.sent();
                    console.error(err_6);
                    return [3 /*break*/, 13];
                case 13: return [2 /*return*/];
            }
        });
    });
}
function handleInputRecord(record) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            //只监控文件上传
            console.log("检测到文件上传");
            if (!isValid(record)) {
                return [2 /*return*/];
            }
            if (transcodeCheck(config)) {
                transcode(record);
            }
            else {
                console.log("转码参数错误");
            }
            return [2 /*return*/];
        });
    });
}
//处理完成转码记录
function handleOutputRecord(record) {
    return __awaiter(this, void 0, void 0, function () {
        var item, err_7, err_8;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 5, , 6]);
                    //只监控文件上传
                    console.log("检测到转码成功文件上传");
                    if (!isValid(record)) {
                        return [2 /*return*/];
                    }
                    item = {
                        appId: record.cos.cosBucket.appid,
                        fileId: getTranscdoeRequestKey(record.cos.cosObject.key),
                        outputFile: record.cos.cosObject.key,
                    };
                    if (!config.stat.enable) return [3 /*break*/, 4];
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, transcodeResultLog(config, [item])];
                case 2:
                    _a.sent();
                    console.log("写入远程日志成功");
                    return [3 /*break*/, 4];
                case 3:
                    err_7 = _a.sent();
                    console.error(err_7);
                    console.error("写入远程日志失败");
                    return [3 /*break*/, 4];
                case 4: return [3 /*break*/, 6];
                case 5:
                    err_8 = _a.sent();
                    console.error(err_8);
                    return [3 /*break*/, 6];
                case 6: return [2 /*return*/];
            }
        });
    });
}
function getDefinitionDiffx(fileId) {
    if (!fileId) {
        return "";
    }
    var reg = /\.f[0-9]+$/;
    var res = fileId.match(reg);
    if (!res) {
        return "";
    }
    return fileId.substring(res['index'] + 1, fileId.length);
}
function transcodeResultLog(config, items) {
    return __awaiter(this, void 0, void 0, function () {
        var logHandler;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    logHandler = null;
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
                    return [4 /*yield*/, logHandler.writeOutputLog({ items: items, config: config })];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function requestTranscodeLog(config, items) {
    return __awaiter(this, void 0, void 0, function () {
        var logHandler;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    logHandler = null;
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
                    return [4 /*yield*/, logHandler.writeInputLog({ items: items, config: config })];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
var videoExtends = new Set([".rmvb", ".mp4", ".3pg", ".mov", ".m4v", ".avi", ".mkv", ".flv", ".vob", ".wmv", ".asf", ".asx", ".dat"]);
//过滤非视频文件
function isValid(record) {
    var objectKey = new ObjectKey(record.cos.cosObject.key);
    if (objectKey.extend && videoExtends.has(objectKey.extend)) {
        return true;
    }
    var contentType = record.cos.cosObject.meta["Content-Type"];
    if (contentType.indexOf("video") != 0) {
        console.log("忽略非视频文件");
        return false;
    }
    return true;
}
var ObjectKey = /** @class */ (function () {
    function ObjectKey(key) {
        var strs = key.split("/");
        this.appId = strs[1];
        this.bucket = strs[2];
        this.path = "/" + strs.slice(3, strs.length - 1).join("/");
        var longName = strs[strs.length - 1];
        var index = longName.lastIndexOf(".");
        if (index < 0) {
            this.name = longName;
            this.extend = "";
        }
        else {
            this.name = longName.substring(0, index);
            this.extend = longName.substring(index, longName.length);
        }
    }
    return ObjectKey;
}());
//请求视频转码
function requestTranscode(queryUrl) {
    return new Promise(function (resolve, reject) {
        var proxy = "";
        if (config.PROXY) {
            proxy = config.PROXY;
        }
        request({ url: queryUrl, timeout: 5000, proxy: proxy }, function (error, response, body) {
            //记录日志
            if (error) {
                reject(error);
            }
            else {
                try {
                    body = JSON.parse(body);
                    if (body.code == 0) {
                        resolve(body);
                    }
                    else {
                        reject(body);
                    }
                }
                catch (error) {
                    reject(error);
                }
            }
        });
    });
}
function getTaskInfo(vodTaskId) {
    return new Promise(function (resolve, reject) {
        var params = {};
        params['Action'] = "GetTaskInfo";
        params['vodTaskId'] = vodTaskId;
        params['Region'] = "gz";
        params['Timestamp'] = Math.round(Date.now() / 1000);
        params['Nonce'] = Math.round(Math.random() * 65535);
        params['SecretId'] = config.SecretId;
        var queryUrl = genUrl(params, config.SecretKey);
        request({ url: queryUrl, timeout: 5000 }, function (error, response, body) {
            //记录日志
            if (error) {
                reject(error);
            }
            else {
                try {
                    body = JSON.parse(body);
                    if (body.code == 0) {
                        resolve(body);
                    }
                    else {
                        reject(body);
                    }
                }
                catch (error) {
                    reject(error);
                }
            }
        });
    });
}
function writeInputLog(config, items) {
    return __awaiter(this, void 0, void 0, function () {
        var cos, item, _i, items_3, item_1, logKey, params;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (items.length == 0) {
                        return [2 /*return*/];
                    }
                    cos = new COS({
                        SecretId: config.SecretId,
                        SecretKey: config.SecretKey,
                    });
                    item = items[0];
                    _i = 0, items_3 = items;
                    _a.label = 1;
                case 1:
                    if (!(_i < items_3.length)) return [3 /*break*/, 4];
                    item_1 = items_3[_i];
                    logKey = "";
                    if (item_1.vodTaskId) {
                        logKey = config.stat.path + "/vodlog/" + config.stat.level + "/SUCESS/" + item_1.fileId + "/" + item_1.vodTaskId + ".csv";
                    }
                    else {
                        logKey = config.stat.path + "/vodlog/" + config.stat.level + "/ERROR/" + item_1.fileId + "/" + crypto.createHash('md5').update(new Date() + "").digest("hex") + ".csv";
                    }
                    params = {
                        Bucket: config.param.output.bucket + "-" + item_1.appId,
                        Region: config.region,
                        Key: logKey,
                    };
                    return [4 /*yield*/, writeRemoteCsv([item_1], cos, params)];
                case 2:
                    _a.sent();
                    _a.label = 3;
                case 3:
                    _i++;
                    return [3 /*break*/, 1];
                case 4: return [2 /*return*/];
            }
        });
    });
}
/*************************************LogCollector ********************************/
var LogCollector = /** @class */ (function () {
    function LogCollector(config) {
        this.cos = new COS({
            "SecretId": config.SecretId,
            "SecretKey": config.SecretKey,
        });
    }
    return LogCollector;
}());
var EachLogCollector = /** @class */ (function (_super) {
    __extends(EachLogCollector, _super);
    function EachLogCollector() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    EachLogCollector.prototype.collectorErrorLog = function (paramsList) {
        return __awaiter(this, void 0, void 0, function () {
            var params, deleteKeys, _i, paramsList_1, params_1, errorItems, errorItem, remoteParam, deleteParams, _a, paramsList_2, params_2, err_9, err_10;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        if (paramsList.length == 0) {
                            return [2 /*return*/];
                        }
                        params = paramsList[0];
                        deleteKeys = [];
                        _b.label = 1;
                    case 1:
                        _b.trys.push([1, 10, , 11]);
                        _i = 0, paramsList_1 = paramsList;
                        _b.label = 2;
                    case 2:
                        if (!(_i < paramsList_1.length)) return [3 /*break*/, 6];
                        params_1 = paramsList_1[_i];
                        return [4 /*yield*/, getRemoteLogItems(this.cos, params_1)];
                    case 3:
                        errorItems = _b.sent();
                        if (!errorItems && errorItems.length == 0) {
                            return [3 /*break*/, 5];
                        }
                        errorItem = errorItems[0];
                        remoteParam = {};
                        remoteParam = Object.assign(remoteParam, params_1);
                        remoteParam.Key = config.stat.path + "/" + errorItem.fileId + ".csv";
                        return [4 /*yield*/, writeRemoteCsv([errorItem], this.cos, remoteParam)];
                    case 4:
                        _b.sent();
                        _b.label = 5;
                    case 5:
                        _i++;
                        return [3 /*break*/, 2];
                    case 6:
                        _b.trys.push([6, 8, , 9]);
                        deleteParams = {};
                        deleteParams = Object.assign(deleteParams, paramsList[0]);
                        deleteParams.Objects = [];
                        for (_a = 0, paramsList_2 = paramsList; _a < paramsList_2.length; _a++) {
                            params_2 = paramsList_2[_a];
                            deleteParams.Objects.push({ Key: params_2.Key });
                        }
                        return [4 /*yield*/, deleteMultipleCosObject(this.cos, deleteParams)];
                    case 7:
                        _b.sent();
                        return [3 /*break*/, 9];
                    case 8:
                        err_9 = _b.sent();
                        console.error(err_9);
                        return [3 /*break*/, 9];
                    case 9: return [3 /*break*/, 11];
                    case 10:
                        err_10 = _b.sent();
                        throw err_10;
                    case 11: return [2 /*return*/];
                }
            });
        });
    };
    EachLogCollector.prototype.collectorSucessLog = function (paramsList) {
        return __awaiter(this, void 0, void 0, function () {
            var params, deleteParamsList, remoteItems, _i, paramsList_3, params_3, sucessItems, sucessItem, orgStatus, needMerge, needDelete, inputs, Prefix, outputParams, outputFile, remoteParam, err_11, deleteParams, _a, deleteParamsList_1, params_4, err_12, err_13;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        if (paramsList.length == 0) {
                            return [2 /*return*/];
                        }
                        params = paramsList[0];
                        deleteParamsList = [];
                        remoteItems = {};
                        _b.label = 1;
                    case 1:
                        _b.trys.push([1, 18, , 19]);
                        _i = 0, paramsList_3 = paramsList;
                        _b.label = 2;
                    case 2:
                        if (!(_i < paramsList_3.length)) return [3 /*break*/, 13];
                        params_3 = paramsList_3[_i];
                        return [4 /*yield*/, getRemoteLogItems(this.cos, params_3)];
                    case 3:
                        sucessItems = _b.sent();
                        if (!sucessItems || sucessItems.length == 0) {
                            return [3 /*break*/, 12];
                        }
                        sucessItem = sucessItems[0];
                        orgStatus = sucessItem.status;
                        _b.label = 4;
                    case 4:
                        _b.trys.push([4, 11, , 12]);
                        needMerge = false;
                        needDelete = false;
                        if (!sucessItem.status) {
                            needMerge = true;
                        }
                        inputs = sucessItem.outputFile.split("/");
                        Prefix = inputs.slice(3, inputs.length).join("/");
                        outputParams = {
                            Bucket: config.param.output.bucket + "-" + config.appId,
                            Region: config.region,
                            Prefix: Prefix
                        };
                        return [4 /*yield*/, getCosBucket(this.cos, outputParams)];
                    case 5:
                        outputFile = _b.sent();
                        if (outputFile && Array.isArray(outputFile.Contents) && outputFile.Contents.length > 0) {
                            sucessItem.status = "FINISH";
                            needMerge = true;
                            needDelete = true;
                        }
                        else {
                            if ((moment().unix() - moment(sucessItem.uploadTime).unix()) > TimeOut) {
                                sucessItem.status = "TimeOut";
                                needMerge = true;
                                needDelete = true;
                            }
                            sucessItem.status = "PROCESSING";
                        }
                        if (!needMerge) return [3 /*break*/, 7];
                        remoteParam = {};
                        remoteParam = Object.assign(remoteParam, params_3);
                        remoteParam.Key = config.stat.path + "/" + sucessItem.fileId + ".csv";
                        return [4 /*yield*/, writeRemoteCsv([sucessItem], this.cos, remoteParam)];
                    case 6:
                        _b.sent();
                        _b.label = 7;
                    case 7:
                        if (!needDelete) return [3 /*break*/, 8];
                        deleteParamsList.push(params_3);
                        return [3 /*break*/, 10];
                    case 8:
                        if (!(orgStatus != sucessItem.status)) return [3 /*break*/, 10];
                        return [4 /*yield*/, writeRemoteCsv([sucessItem], this.cos, params_3)];
                    case 9:
                        _b.sent();
                        _b.label = 10;
                    case 10: return [3 /*break*/, 12];
                    case 11:
                        err_11 = _b.sent();
                        console.error(err_11);
                        return [3 /*break*/, 12];
                    case 12:
                        _i++;
                        return [3 /*break*/, 2];
                    case 13:
                        _b.trys.push([13, 16, , 17]);
                        deleteParams = {};
                        deleteParams = Object.assign(deleteParams, deleteParamsList[0]);
                        deleteParams.Objects = [];
                        for (_a = 0, deleteParamsList_1 = deleteParamsList; _a < deleteParamsList_1.length; _a++) {
                            params_4 = deleteParamsList_1[_a];
                            deleteParams.Objects.push({ Key: params_4.Key });
                        }
                        if (!(deleteParams.Objects.length > 0)) return [3 /*break*/, 15];
                        return [4 /*yield*/, deleteMultipleCosObject(this.cos, deleteParams)];
                    case 14:
                        _b.sent();
                        _b.label = 15;
                    case 15: return [3 /*break*/, 17];
                    case 16:
                        err_12 = _b.sent();
                        console.error(err_12);
                        return [3 /*break*/, 17];
                    case 17: return [3 /*break*/, 19];
                    case 18:
                        err_13 = _b.sent();
                        throw (err_13);
                    case 19: return [2 /*return*/];
                }
            });
        });
    };
    return EachLogCollector;
}(LogCollector));
var DayLogCollector = /** @class */ (function (_super) {
    __extends(DayLogCollector, _super);
    function DayLogCollector() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    DayLogCollector.prototype.collectorErrorLog = function (paramsList) {
        return __awaiter(this, void 0, void 0, function () {
            var params, remoteItems, _i, paramsList_4, params_5, errorItems, errorItem, Day, remoteCosParams, _a, _b, err_14, _c, _d, _e, dayKey, remoteItem, remoteCosParams, deleteParams, _f, paramsList_5, params_6, err_15, err_16;
            return __generator(this, function (_g) {
                switch (_g.label) {
                    case 0:
                        if (paramsList.length == 0) {
                            return [2 /*return*/];
                        }
                        params = paramsList[0];
                        remoteItems = {};
                        _g.label = 1;
                    case 1:
                        _g.trys.push([1, 17, , 18]);
                        _i = 0, paramsList_4 = paramsList;
                        _g.label = 2;
                    case 2:
                        if (!(_i < paramsList_4.length)) return [3 /*break*/, 9];
                        params_5 = paramsList_4[_i];
                        return [4 /*yield*/, getRemoteLogItems(this.cos, params_5)];
                    case 3:
                        errorItems = _g.sent();
                        if (!errorItems && errorItems.length == 0) {
                            return [3 /*break*/, 8];
                        }
                        errorItem = errorItems[0];
                        Day = moment(errorItem.uploadTime).format("YYYY-MM-DD");
                        if (!!remoteItems[Day]) return [3 /*break*/, 7];
                        remoteCosParams = {
                            Bucket: params_5.Bucket,
                            Region: params_5.Region,
                            Key: config.stat.path + "/" + Day + ".csv"
                        };
                        _g.label = 4;
                    case 4:
                        _g.trys.push([4, 6, , 7]);
                        _a = remoteItems;
                        _b = Day;
                        return [4 /*yield*/, getRemoteLogItems(this.cos, remoteCosParams)];
                    case 5:
                        _a[_b] = _g.sent();
                        return [3 /*break*/, 7];
                    case 6:
                        err_14 = _g.sent();
                        remoteItems[Day] = [];
                        return [3 /*break*/, 7];
                    case 7:
                        remoteItems[Day] = LogItem.mergeLogItemsWithFileId(remoteItems[Day], errorItems);
                        _g.label = 8;
                    case 8:
                        _i++;
                        return [3 /*break*/, 2];
                    case 9:
                        _c = [];
                        for (_d in remoteItems)
                            _c.push(_d);
                        _e = 0;
                        _g.label = 10;
                    case 10:
                        if (!(_e < _c.length)) return [3 /*break*/, 13];
                        dayKey = _c[_e];
                        remoteItem = remoteItems[dayKey];
                        remoteCosParams = {
                            Bucket: params.Bucket,
                            Region: params.Region,
                            Key: config.stat.path + "/" + dayKey + ".csv"
                        };
                        return [4 /*yield*/, writeRemoteCsv(remoteItem, this.cos, remoteCosParams)];
                    case 11:
                        _g.sent();
                        _g.label = 12;
                    case 12:
                        _e++;
                        return [3 /*break*/, 10];
                    case 13:
                        _g.trys.push([13, 15, , 16]);
                        deleteParams = {};
                        deleteParams = Object.assign(deleteParams, paramsList[0]);
                        deleteParams.Objects = [];
                        for (_f = 0, paramsList_5 = paramsList; _f < paramsList_5.length; _f++) {
                            params_6 = paramsList_5[_f];
                            deleteParams.Objects.push({ Key: params_6.Key });
                        }
                        return [4 /*yield*/, deleteMultipleCosObject(this.cos, deleteParams)];
                    case 14:
                        _g.sent();
                        return [3 /*break*/, 16];
                    case 15:
                        err_15 = _g.sent();
                        console.error(err_15);
                        return [3 /*break*/, 16];
                    case 16: return [3 /*break*/, 18];
                    case 17:
                        err_16 = _g.sent();
                        throw err_16;
                    case 18: return [2 /*return*/];
                }
            });
        });
    };
    DayLogCollector.prototype.collectorSucessLog = function (paramsList) {
        return __awaiter(this, void 0, void 0, function () {
            var params, deleteParamsList, remoteItems, _i, paramsList_6, params_7, sucessItems, sucessItem, orgStatus, needMerge, needDelete, inputs, Prefix, outputParams, outputFile, Day, remoteCosParams, _a, _b, err_17, err_18, _c, _d, _e, dayKey, remoteItem, remoteCosParams, deleteParams, _f, deleteParamsList_2, params_8, err_19, err_20;
            return __generator(this, function (_g) {
                switch (_g.label) {
                    case 0:
                        if (paramsList.length == 0) {
                            return [2 /*return*/];
                        }
                        params = paramsList[0];
                        deleteParamsList = [];
                        remoteItems = {};
                        _g.label = 1;
                    case 1:
                        _g.trys.push([1, 24, , 25]);
                        _i = 0, paramsList_6 = paramsList;
                        _g.label = 2;
                    case 2:
                        if (!(_i < paramsList_6.length)) return [3 /*break*/, 16];
                        params_7 = paramsList_6[_i];
                        return [4 /*yield*/, getRemoteLogItems(this.cos, params_7)];
                    case 3:
                        sucessItems = _g.sent();
                        if (!sucessItems || sucessItems.length == 0) {
                            return [3 /*break*/, 15];
                        }
                        sucessItem = sucessItems[0];
                        orgStatus = sucessItem.status;
                        _g.label = 4;
                    case 4:
                        _g.trys.push([4, 14, , 15]);
                        needMerge = false;
                        needDelete = false;
                        if (!sucessItem.status) {
                            needMerge = true;
                        }
                        inputs = sucessItem.outputFile.split("/");
                        Prefix = inputs.slice(3, inputs.length).join("/");
                        outputParams = {
                            Bucket: config.param.output.bucket + "-" + config.appId,
                            Region: config.region,
                            Prefix: Prefix
                        };
                        return [4 /*yield*/, getCosBucket(this.cos, outputParams)];
                    case 5:
                        outputFile = _g.sent();
                        if (outputFile && Array.isArray(outputFile.Contents) && outputFile.Contents.length > 0) {
                            sucessItem.status = "FINISH";
                            needMerge = true;
                            needDelete = true;
                        }
                        else {
                            if ((moment().unix() - moment(sucessItem.uploadTime).unix()) > TimeOut) {
                                sucessItem.status = "TimeOut";
                                needMerge = true;
                                needDelete = true;
                            }
                            sucessItem.status = "PROCESSING";
                        }
                        if (!needMerge) return [3 /*break*/, 10];
                        Day = moment(sucessItem.uploadTime).format("YYYY-MM-DD");
                        if (!!remoteItems[Day]) return [3 /*break*/, 9];
                        remoteCosParams = {
                            Bucket: params_7.Bucket,
                            Region: params_7.Region,
                            Key: config.stat.path + "/" + Day + ".csv"
                        };
                        _g.label = 6;
                    case 6:
                        _g.trys.push([6, 8, , 9]);
                        _a = remoteItems;
                        _b = Day;
                        return [4 /*yield*/, getRemoteLogItems(this.cos, remoteCosParams)];
                    case 7:
                        _a[_b] = _g.sent();
                        return [3 /*break*/, 9];
                    case 8:
                        err_17 = _g.sent();
                        remoteItems[Day] = [];
                        return [3 /*break*/, 9];
                    case 9:
                        remoteItems[Day] = LogItem.mergeLogItemsWithFileId(remoteItems[Day], [sucessItem]);
                        _g.label = 10;
                    case 10:
                        if (!needDelete) return [3 /*break*/, 11];
                        deleteParamsList.push(params_7);
                        return [3 /*break*/, 13];
                    case 11:
                        if (!(orgStatus != sucessItem.status)) return [3 /*break*/, 13];
                        return [4 /*yield*/, writeRemoteCsv([sucessItem], this.cos, params_7)];
                    case 12:
                        _g.sent();
                        _g.label = 13;
                    case 13: return [3 /*break*/, 15];
                    case 14:
                        err_18 = _g.sent();
                        console.error(err_18);
                        return [3 /*break*/, 15];
                    case 15:
                        _i++;
                        return [3 /*break*/, 2];
                    case 16:
                        _c = [];
                        for (_d in remoteItems)
                            _c.push(_d);
                        _e = 0;
                        _g.label = 17;
                    case 17:
                        if (!(_e < _c.length)) return [3 /*break*/, 20];
                        dayKey = _c[_e];
                        remoteItem = remoteItems[dayKey];
                        remoteCosParams = {
                            Bucket: params.Bucket,
                            Region: params.Region,
                            Key: config.stat.path + "/" + dayKey + ".csv"
                        };
                        return [4 /*yield*/, writeRemoteCsv(remoteItem, this.cos, remoteCosParams)];
                    case 18:
                        _g.sent();
                        _g.label = 19;
                    case 19:
                        _e++;
                        return [3 /*break*/, 17];
                    case 20:
                        _g.trys.push([20, 22, , 23]);
                        deleteParams = {};
                        deleteParams = Object.assign(deleteParams, deleteParamsList[0]);
                        deleteParams.Objects = [];
                        for (_f = 0, deleteParamsList_2 = deleteParamsList; _f < deleteParamsList_2.length; _f++) {
                            params_8 = deleteParamsList_2[_f];
                            deleteParams.Objects.push({ Key: params_8.Key });
                        }
                        return [4 /*yield*/, deleteMultipleCosObject(this.cos, deleteParams)];
                    case 21:
                        _g.sent();
                        return [3 /*break*/, 23];
                    case 22:
                        err_19 = _g.sent();
                        console.error(err_19);
                        return [3 /*break*/, 23];
                    case 23: return [3 /*break*/, 25];
                    case 24:
                        err_20 = _g.sent();
                        throw (err_20);
                    case 25: return [2 /*return*/];
                }
            });
        });
    };
    return DayLogCollector;
}(LogCollector));
function collectorDayLog(config) {
    return __awaiter(this, void 0, void 0, function () {
        var cos, dayLogCollector, errorParams, errResult, errDayParamsList, _i, _a, content, err_21, successParams, successResult, successDayParamsList, _b, _c, content, err_22;
        return __generator(this, function (_d) {
            switch (_d.label) {
                case 0:
                    cos = new COS({
                        "SecretId": config.SecretId,
                        "SecretKey": config.SecretKey,
                    });
                    dayLogCollector = new DayLogCollector(config);
                    errorParams = {
                        Bucket: config.param.output.bucket + "-" + config.appId,
                        Region: config.region,
                        Prefix: config.stat.path + "/vodlog/DAY/ERROR/",
                    };
                    return [4 /*yield*/, getCosBucket(cos, errorParams)];
                case 1:
                    errResult = _d.sent();
                    errDayParamsList = [];
                    if (!(errResult && Array.isArray(errResult.Contents))) return [3 /*break*/, 5];
                    for (_i = 0, _a = errResult.Contents; _i < _a.length; _i++) {
                        content = _a[_i];
                        errDayParamsList.push({
                            Bucket: errorParams.Bucket,
                            Region: errorParams.Region,
                            Key: content.Key,
                        });
                    }
                    _d.label = 2;
                case 2:
                    _d.trys.push([2, 4, , 5]);
                    return [4 /*yield*/, dayLogCollector.collectorErrorLog(errDayParamsList)];
                case 3:
                    _d.sent();
                    return [3 /*break*/, 5];
                case 4:
                    err_21 = _d.sent();
                    console.error(err_21);
                    return [3 /*break*/, 5];
                case 5:
                    successParams = {
                        Bucket: config.param.output.bucket + "-" + config.appId,
                        Region: config.region,
                        Prefix: config.stat.path + "/vodlog/DAY/SUCESS/",
                    };
                    return [4 /*yield*/, getCosBucket(cos, successParams)];
                case 6:
                    successResult = _d.sent();
                    successDayParamsList = [];
                    if (!(successResult && Array.isArray(successResult.Contents))) return [3 /*break*/, 10];
                    for (_b = 0, _c = successResult.Contents; _b < _c.length; _b++) {
                        content = _c[_b];
                        successDayParamsList.push({
                            Bucket: successParams.Bucket,
                            Region: successParams.Region,
                            Key: content.Key,
                        });
                    }
                    _d.label = 7;
                case 7:
                    _d.trys.push([7, 9, , 10]);
                    return [4 /*yield*/, dayLogCollector.collectorSucessLog(successDayParamsList)];
                case 8:
                    _d.sent();
                    return [3 /*break*/, 10];
                case 9:
                    err_22 = _d.sent();
                    console.error(err_22);
                    return [3 /*break*/, 10];
                case 10: return [2 /*return*/];
            }
        });
    });
}
/******************************** LogHandler *************************************/
var LogHandler = /** @class */ (function () {
    function LogHandler() {
    }
    return LogHandler;
}());
//按请求记录日志
var EachLogHandler = /** @class */ (function (_super) {
    __extends(EachLogHandler, _super);
    function EachLogHandler() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    EachLogHandler.prototype.writeInputLog = function (_a) {
        var items = _a.items, config = _a.config;
        return __awaiter(this, void 0, void 0, function () {
            var cos, item, params;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        //直接覆盖源文件
                        if (!items || items.length == 0) {
                            return [2 /*return*/];
                        }
                        cos = new COS({
                            SecretId: config.SecretId,
                            SecretKey: config.SecretKey,
                        });
                        item = items[0];
                        params = {
                            Bucket: config.param.output.bucket + "-" + item.appId,
                            Region: config.region,
                            Key: getTranscdoeRequestKey(item.inputFile, config) + ".csv",
                        };
                        return [4 /*yield*/, writeRemoteCsv(items, cos, params)];
                    case 1:
                        _b.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    EachLogHandler.prototype.writeOutputLog = function (_a) {
        var items = _a.items, config = _a.config;
        return __awaiter(this, void 0, void 0, function () {
            var cos, item, key, reg, res, params, remoteItems, err_23;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        //直接覆盖源文件
                        if (!items || items.length == 0) {
                            return [2 /*return*/];
                        }
                        cos = new COS({
                            SecretId: config.SecretId,
                            SecretKey: config.SecretKey,
                        });
                        item = items[0];
                        key = getTranscdoeRequestKey(item.outputFile);
                        reg = /\.f[0-9]+$/;
                        res = key.match(reg);
                        if (res) {
                            key = key.substring(0, res['index']);
                        }
                        _b.label = 1;
                    case 1:
                        _b.trys.push([1, 5, , 6]);
                        params = {
                            Bucket: config.param.output.bucket + "-" + item.appId,
                            Region: config.region,
                            Key: key + ".csv",
                        };
                        return [4 /*yield*/, getRemoteLogItems(cos, params)];
                    case 2:
                        remoteItems = _b.sent();
                        return [4 /*yield*/, LogItem.mergeLogItemsWithFileId(remoteItems, items, false)];
                    case 3:
                        remoteItems = _b.sent();
                        return [4 /*yield*/, writeRemoteCsv(remoteItems, cos, params)];
                    case 4:
                        _b.sent();
                        return [3 /*break*/, 6];
                    case 5:
                        err_23 = _b.sent();
                        console.error(err_23);
                        return [3 /*break*/, 6];
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    return EachLogHandler;
}(LogHandler));
//按天记录日志
var DayLogHander = /** @class */ (function (_super) {
    __extends(DayLogHander, _super);
    function DayLogHander() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    DayLogHander.prototype.writeInputLog = function (_a) {
        var items = _a.items, config = _a.config;
        return __awaiter(this, void 0, void 0, function () {
            var logKey, cos, item, params, remoteItems, err_24, err_25;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        if (!items || items.length == 0) {
                            return [2 /*return*/];
                        }
                        logKey = moment().format("YYYY-MM-DD") + ".csv";
                        cos = new COS({
                            SecretId: config.SecretId,
                            SecretKey: config.SecretKey,
                        });
                        item = items[0];
                        params = {
                            Bucket: config.param.output.bucket + "-" + item.appId,
                            Region: config.region,
                            Key: logKey,
                        };
                        remoteItems = null;
                        _b.label = 1;
                    case 1:
                        _b.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, getRemoteLogItems(cos, params)];
                    case 2:
                        remoteItems = _b.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        err_24 = _b.sent();
                        return [3 /*break*/, 4];
                    case 4:
                        if (!remoteItems) {
                            remoteItems = [];
                        }
                        _b.label = 5;
                    case 5:
                        _b.trys.push([5, 7, , 8]);
                        //2 更新日志内容
                        items = LogItem.mergeLogItemsWithFileId(remoteItems, items);
                        //3 写入远程日志
                        return [4 /*yield*/, writeRemoteCsv(items, cos, params)];
                    case 6:
                        //3 写入远程日志
                        _b.sent();
                        return [3 /*break*/, 8];
                    case 7:
                        err_25 = _b.sent();
                        console.error(err_25);
                        return [3 /*break*/, 8];
                    case 8: return [2 /*return*/];
                }
            });
        });
    };
    DayLogHander.prototype.writeOutputLog = function (_a) {
        var items = _a.items, config = _a.config;
        return __awaiter(this, void 0, void 0, function () {
            var cos, item, todayLogKey, params, todayRemoteItems, err_26, length, yesterdayLogKey, yesterdayRemoteItems, err_27;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        //拉取今天的更新
                        if (!items || items.length == 0) {
                            return [2 /*return*/];
                        }
                        cos = new COS({
                            SecretId: config.SecretId,
                            SecretKey: config.SecretKey,
                        });
                        item = items[0];
                        todayLogKey = moment().format("YYYY-MM-DD") + ".csv";
                        params = {
                            Bucket: config.param.output.bucket + "-" + item.appId,
                            Region: config.region,
                            Key: todayLogKey,
                        };
                        todayRemoteItems = null;
                        _b.label = 1;
                    case 1:
                        _b.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, getRemoteLogItems(cos, params)];
                    case 2:
                        todayRemoteItems = _b.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        err_26 = _b.sent();
                        return [3 /*break*/, 4];
                    case 4:
                        if (!todayRemoteItems) {
                            todayRemoteItems = [];
                        }
                        length = todayRemoteItems.length;
                        todayRemoteItems = LogItem.mergeLogItemsWithFileId(todayRemoteItems, items, false);
                        if (!items[0]._FIND) return [3 /*break*/, 6];
                        return [4 /*yield*/, writeRemoteCsv(todayRemoteItems, cos, params)];
                    case 5:
                        _b.sent();
                        return [2 /*return*/];
                    case 6:
                        yesterdayLogKey = moment().subtract(1, 'days').format("YYYY-MM-DD") + ".csv";
                        params = {
                            Bucket: config.param.output.bucket + "-" + item.appId,
                            Region: config.region,
                            Key: todayLogKey,
                        };
                        yesterdayRemoteItems = null;
                        _b.label = 7;
                    case 7:
                        _b.trys.push([7, 9, , 10]);
                        return [4 /*yield*/, getRemoteLogItems(cos, params)];
                    case 8:
                        yesterdayRemoteItems = _b.sent();
                        return [3 /*break*/, 10];
                    case 9:
                        err_27 = _b.sent();
                        return [3 /*break*/, 10];
                    case 10:
                        if (!yesterdayRemoteItems) {
                            return [2 /*return*/];
                        }
                        length = todayRemoteItems.length();
                        yesterdayRemoteItems = LogItem.mergeLogItemsWithFileId(yesterdayRemoteItems, items, false);
                        if (!items[0]._FIND) return [3 /*break*/, 12];
                        return [4 /*yield*/, writeRemoteCsv(yesterdayRemoteItems, cos, params)];
                    case 11:
                        _b.sent();
                        return [2 /*return*/];
                    case 12: return [2 /*return*/];
                }
            });
        });
    };
    return DayLogHander;
}(LogHandler));
/***************************************************utils ****************/
function normlizePath(path) {
    if (!path) {
        return "";
    }
    if (path === "/") {
        return path;
    }
    var splits = path.split("/").filter(function (str) {
        return str != "";
    });
    return splits.join("/");
}
function getTranscdoeRequestKey(filePath, config) {
    if (config === void 0) { config = null; }
    var fileIds = [];
    var file = new ObjectKey(filePath);
    var strs = [];
    strs.push(file.appId);
    if (config && config.param.output.dir) {
        strs = strs.concat(config.param.output.dir.split("/"));
    }
    else {
        strs = strs.concat(file.path.split("/"));
    }
    strs.push(file.name);
    return strs.join("_");
}
//获取所有文件ID
function getTranscodeFileIds(filePath, config) {
    var fileIds = [];
    var requestKey = getTranscdoeRequestKey(filePath, config);
    for (var _i = 0, _a = config.param.mediaProcess.transcode.definition; _i < _a.length; _i++) {
        var definition = _a[_i];
        fileIds.push(requestKey + ".f" + definition);
    }
    return fileIds;
}
function genQueryUrl(record) {
    var params = {};
    params['input.bucket'] = record.cos.cosBucket.name;
    var inputs = record.cos.cosObject.key.split("/");
    params['input.path'] = "/" + inputs.slice(3).join("/");
    params['output.bucket'] = config.param.output.bucket;
    if (config.param.output.dir) {
        params['output.dir'] = config.param.output.dir;
    }
    else {
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
    var queryUrl = genUrl(params, config.SecretKey);
    console.log("生成请求");
    console.log(queryUrl);
    return queryUrl;
}
function genUrl(params, secretKey) {
    var keys = Object.keys(params);
    keys.sort();
    var qstr = "";
    keys.forEach(function (key) {
        var val = params[key];
        if (val === undefined || val === null || (typeof val === 'number' && isNaN(val))) {
            val = '';
        }
        qstr += '&' + (key.indexOf('_') ? key.replace(/_/g, '.') : key) + '=' + val;
    });
    qstr = qstr.slice(1);
    var signature = sign("GET" + "vod.api.qcloud.com/v2/index.php" + '?' + qstr, secretKey);
    params.Signature = signature;
    return url + "?" + qs.stringify(params);
}
function sign(str, secretKey) {
    var hmac = crypto.createHmac('sha1', secretKey || '');
    return hmac.update(new Buffer(str, 'utf8')).digest('base64');
}
/****************************cos async/await  */
function getCosBucket(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.getBucket(params, function (err, data) {
            if (err) {
                reject(err);
            }
            else {
                resolve(data);
            }
        });
    });
}
function isCosObjectExist(cos, params) {
    return __awaiter(this, void 0, void 0, function () {
        var result, err_28;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 2, , 3]);
                    return [4 /*yield*/, headCosObject(cos, params)];
                case 1:
                    result = _a.sent();
                    return [2 /*return*/, true];
                case 2:
                    err_28 = _a.sent();
                    return [2 /*return*/, false];
                case 3: return [2 /*return*/];
            }
        });
    });
}
function headCosObject(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.headObject(params, function (err, data) {
            if (err) {
                reject(err);
            }
            else {
                if (data.statusCode == 200) {
                    resolve(data.headers);
                }
                else {
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
                reject(err);
            }
            else {
                resolve(data);
            }
        });
    });
}
function deleteMultipleCosObject(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.deleteMultipleObject(params, function (err, data) {
            if (err) {
                reject(err);
            }
            else {
                resolve(data);
            }
        });
    });
}
function getCosObject(cos, params) {
    return new Promise(function (resolve, reject) {
        cos.getObject(params, function (err, data) {
            if (err) {
                reject(err);
            }
            else {
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
            }
            else {
                resolve(data);
            }
        });
    });
}
/***************remote csv tools */
//写入远程Csv文件
function writeRemoteCsv(items, cos, params) {
    return new Promise(function (resolve, reject) {
        var logFile = "/tmp/" + crypto.createHash('md5').update(new Date() + "").digest("hex") + ".csv";
        var csvStream = csv.createWriteStream({ headers: true });
        var writableStream = fs.createWriteStream(logFile);
        writableStream.on("finish", function () {
            return __awaiter(this, void 0, void 0, function () {
                var result, i, err_29;
                return __generator(this, function (_a) {
                    switch (_a.label) {
                        case 0:
                            params['Body'] = fs.createReadStream(logFile);
                            params["ContentLength"] = fs.statSync(logFile).size;
                            result = null;
                            i = 0;
                            _a.label = 1;
                        case 1:
                            if (!(i < RetryNum)) return [3 /*break*/, 7];
                            _a.label = 2;
                        case 2:
                            _a.trys.push([2, 4, , 5]);
                            return [4 /*yield*/, putCosObject(cos, params)];
                        case 3:
                            result = _a.sent();
                            return [3 /*break*/, 5];
                        case 4:
                            err_29 = _a.sent();
                            console.error(err_29);
                            return [3 /*break*/, 6];
                        case 5: return [3 /*break*/, 7];
                        case 6:
                            i++;
                            return [3 /*break*/, 1];
                        case 7:
                            if (result == null) {
                                reject({ message: "远程日志写入失败" });
                            }
                            else {
                                resolve({ message: "远程日志写入成功" });
                            }
                            try {
                                fs.unlinkSync(logFile);
                            }
                            catch (err) {
                            }
                            return [2 /*return*/];
                    }
                });
            });
        });
        writableStream.on("error", function (err) {
            reject({ message: "写入 csv 文件失败" });
        });
        csvStream.pipe(writableStream);
        for (var _i = 0, items_4 = items; _i < items_4.length; _i++) {
            var item = items_4[_i];
            if (typeof item.getObj == "function") {
                csvStream.write(item.getObj());
            }
            else {
                csvStream.write(item);
            }
        }
        csvStream.end();
    });
}
//获取远程日志文件项目
function getRemoteLogItems(cos, params) {
    return new Promise(function (resolve, reject) {
        return __awaiter(this, void 0, void 0, function () {
            var items, remoteData, i, exist, err_30;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        items = [];
                        remoteData = null;
                        i = 0;
                        _a.label = 1;
                    case 1:
                        if (!(i < RetryNum)) return [3 /*break*/, 9];
                        _a.label = 2;
                    case 2:
                        _a.trys.push([2, 6, , 7]);
                        return [4 /*yield*/, isCosObjectExist(cos, params)];
                    case 3:
                        exist = _a.sent();
                        if (!exist) return [3 /*break*/, 5];
                        return [4 /*yield*/, getCosObject(cos, params)];
                    case 4:
                        remoteData = _a.sent();
                        _a.label = 5;
                    case 5: return [3 /*break*/, 7];
                    case 6:
                        err_30 = _a.sent();
                        return [3 /*break*/, 8];
                    case 7: return [3 /*break*/, 9];
                    case 8:
                        i++;
                        return [3 /*break*/, 1];
                    case 9:
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
                        return [2 /*return*/];
                }
            });
        });
    });
}

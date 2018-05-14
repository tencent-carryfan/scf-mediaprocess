//测试视频上传调用专转码

const fs = require("fs");
const main_handler = require('./index.js').main_handler;


const event = {
    "Records": [
        {
            "cos": {
                "cosBucket": {
                    "appid": "1251132654",
                    "name": "testin0511",
                    "region": "bj"
                },
                "cosNotificationId": "unkown",
                "cosObject": {
                    "key": "/1251132654/input/input/test.mp4",
                    "meta": {
                        "Content-Type": "video/mp4",
                        "x-cos-request-id": "NWFjOWNiNmRfZTI4NWQ2NF84MTRkX2E3OTJh"
                    },
                    "size": 56345,
                    "url": "https://testin0511-1251132654.cos.ap-beijing.myqcloud.com/input/test.mp4",
                    "vid": ""
                },
                "cosSchemaVersion": "1.0"
            },
            "event": {
                "eventName": "cos:ObjectCreated:Put",
                "eventQueue": "qcs:0:lambda:bj:appid/1256244234:upload",
                "eventSource": "qcs::cos",
                "eventTime": 1523174253,
                "eventVersion": "1.0",
                "reqid": 243077449,
                "requestParameters": {
                    "requestHeaders": {
                        "Authorization": "q-sign-algorithm=sha1&q-ak=AKIDukNnlKv3a8WTGTkIMwJ1yt6tgrQZ5TOc&q-sign-time=1523174252;1523176052&q-key-time=1523174252;1523176052&q-header-list=&q-url-param-list=&q-signature=66e8ab4a4681dbcb2b65276fd4e358a90ac48909"
                    },
                    "requestSourceIP": "14.17.22.34"
                },
                "reservedInfo": ""
            }
        }
    ]
}




const loadConfig = require('./index.js').loadConfig;

let config = loadConfig();
config.type = "input";
(async function(){
    await main_handler(event,"","",config);
    //await main_handler(eventWithExtend);
})();


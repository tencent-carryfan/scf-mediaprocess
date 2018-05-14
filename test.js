





let path = "/";




let splits = path.split("/").filter(function(str){

    return str!="";
});

console.log("/"+splits.join("/")+"/");


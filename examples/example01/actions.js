
function onInsert(doc) {
    console.log("insert", doc);
    return true;
}
function onDelete(doc) {
    console.log("delete", doc);
    return true;
}
function onUpdate(doc) {
    return true;
}
function onReplace(doc) {
    return true;
}
function onInvalidate(doc) {
    return true;
}
function healthCheck() {
    return true;
}

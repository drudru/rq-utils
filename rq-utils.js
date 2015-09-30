var net = require('net');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;
// TODO
// enum for status
// objects for msg and msg-id
// logging with levels
var RQ = (function () {
    function RQ(rq_base_dir) {
        this.rq_base_dir = rq_base_dir;
    }
    RQ.prototype.create_rq_packet = function (cmd, payload) {
        var packet = ["rq1"];
        var json = JSON.stringify(payload);
        var data = cmd + " " + json;
        var buff_len = Buffer.byteLength(data, 'utf8');
        packet.push(sprintf("%08d", buff_len));
        packet.push(data);
        return packet.join(' ');
    };
    RQ.prototype.send_packet = function (queue_name, packet, callback) {
        // Open unix domain socket
        var que_path = path.join(this.rq_base_dir, 'queue', queue_name, 'queue.sock');
        var client = net.createConnection(que_path);
        console.log(que_path);
        client.on("connect", function () {
            console.log(packet);
            client.write(packet);
        });
        var body = [];
        client.on('data', function (d) { body.push(d); });
        client.on('close', function () { });
        client.on('error', function (e) { console.log('got error', e); });
        client.on('end', function () {
            var resp = body.join('');
            console.log("Got response: " + resp);
            var parts = [resp.slice(0, 4), resp.slice(4, 12), resp.slice(13)];
            if (parts[0] != 'rq1 ')
                return callback('invalid protocol', null);
            var len = parseInt(parts[1], 10);
            if (len != Buffer.byteLength(parts[2], 'utf8'))
                return callback('invalid response - len mismatch', null);
            var msg = JSON.parse(parts[2]);
            if (msg[0] == "ok")
                callback(null, msg[1]);
            else
                callback("server", msg);
        });
    };
    RQ.prototype.get_status = function (msg_id, callback) {
        // TODO: verify parms - create an object
        var parts = msg_id.split('/'); // Full msg-id
        var queue_name = parts[4];
        var payload = { msg_id: parts[5] };
        var packet = this.create_rq_packet("get_message_status", payload);
        this.send_packet(queue_name, packet, function (err, result) {
            if (err)
                return callback(err, null);
            var idx = result.status.indexOf(' - ');
            return [result["status"].slice(0, idx), result["status"].slice(idx + 3)];
        });
    };
    RQ.prototype.create_message = function (msg, callback) {
        // TODO: verify parms - create an object
        var queue_name = msg["dest"]; // Just the queue name
        var packet = this.create_rq_packet("create_message", msg);
        this.send_packet(queue_name, packet, callback);
    };
    return RQ;
})();
function initializer(rq_dir) {
    return new RQ(rq_dir);
}
module.exports = initializer;

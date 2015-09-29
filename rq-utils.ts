

var net = require('net');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;


// TODO
// enum for status
// objects for msg and msg-id

class RQ
{
  rq_base_dir:string;

  constructor(rq_base_dir:string)
  {
      this.rq_base_dir = rq_base_dir;
  }
  
  private create_rq_packet(cmd:string, payload:any):string
  {
      let packet = ["rq1"];
      var json = JSON.stringify(payload);
      var data = cmd + " " + json;
      var buff_len = Buffer.byteLength(data, 'utf8');
      packet.push(sprintf("%08d", buff_len));
      packet.push(data);
      
      return packet.join(' ');
  }
  
  private send_packet(queue_name:string, packet:string, callback:(err:string, result:any) => void):any
  {
    // Open unix domain socket
    var que_path = path.join(this.rq_base_dir, 'queue', queue_name, 'queue.sock');
    var client = net.createConnection(que_path);
    console.log(que_path);
    client.on("connect", function() {
      console.log(packet);
      client.write(packet);
    });
    
    var body = [];
    client.on('data', function (d) { body.push(d) });
    client.on('close', function () { });
    client.on('error', function (e) { console.log('got error', e) });
    client.on('end', function () {
      var resp = body.join('');
      console.log("Got response: " + resp);
  
      var parts = [ resp.slice(0,4), resp.slice(4,12), resp.slice(13) ];
    
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
  
  }
  
  get_status(msg_id:string, callback:(err:string,status:string[]) => void):void
  {
    // TODO: verify parms - create an object
    var parts = msg_id.split('/');  // Full msg-id
    var queue_name = parts[4];
  
  
    var payload = { msg_id: parts[5] }
    var packet = this.create_rq_packet("get_message_status", payload);
    
    this.send_packet(queue_name, packet, (err:string, result:any) => {
      if (err)
        return callback(err, null);
      var idx = result.status.indexOf(' - '); 
      return [status["status"].slice(0, idx), status["status"].slice(idx + 3)];
    });
  }

  create_message(msg:any, callback:(err:string,status:string[]) => void):void
  {
    // TODO: verify parms - create an object
    var queue_name = msg["dest"];  // Just the queue name
  
    var packet = this.create_rq_packet("create_message", msg);
    
    this.send_packet(queue_name, packet, callback);
  }
  
}


function initializer(rq_dir)
{
  return new RQ(rq_dir);
}

module.exports = initializer;

var db = require('./db');
// подключение конфигурации
var config = require("./config/config");

var domain = require('domain');
var express = require('express');
var async = require('async');
var fs = require('fs');
const util = require('util');

// библиотека для работы с firebird 2.1-2.5
var Firebird = require('node-firebird');
var md5 = require("md5");

var Redis = require("redis");


var bodyParser = require('body-parser');
var app = express();

var d = domain.create();

// Конфигурация БД Firebird
var options = {};

options.host = config.host_fb;
options.port = config.port_fb;
options.database = config.database_fb;
options.user = config.dbuser_fb;
options.password = config.dbpwd_fb;
options.lowercase_keys = false; // set to true to lowercase keys
options.role = null;            // default
options.pageSize = 4096;        // default when creating database
//**********

// Обработка ошибок доменом и запись их в фаил логов
d.on('error',function(err)
{ 
var curr_date = new Date();
var log_str = '['+curr_date.toString()+'] '+'Произошла Ошибка - '+err;
//fs.writeFile('./logs/error_log.txt',log_str.toString());
console.log('ERROR - '+err );

});
//*****
var client = Redis.createClient(config.redis_port,config.redis_host);
// Обьекты которые обработает Домен
d.run(function(){

// инициализация клиента Redis


app.listen(config.srv_port, function(){
console.log('SERVER STARTED on port '+config.srv_port);
});



});
//******

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));


//******* Вход в API
app.post('/API',function(req, res){

var result = req.body;
var command = result.command;

switch(command) {
    case 'really_aut' :  /*Команда проверки доступности хэш ключа*/
        var hash_key = result.hash_key;
        var answer;
        client.get(hash_key, function (err, ans) {


            if (!(util.isNull(ans))) {
                answer =
                    {
                        "success": true,
                        "error": "",
                        "error_code": 0,
                        "data": ans
                    };
            } else {
                answer =
                    {
                        "success": false,
                        "error": "not found",
                        "error_code": 1,
                        "data": null
                    };

            }
            res.setHeader("content-type", "text/json");
            res.send(answer);
        });

        break;
    case 'set_autorize' :  /* Команда авторизации в Приложении*/
        var login = result.login;
        var passwd = result.passwd;
        var success = true;
        var error_code = 0;
        var error = "";

        var cur_date = new Date();

        Firebird.attach(options, function (err, db) {
            if (err)
                throw err;
            // db = DATABASE
            db.query("select w.Working,w.worker_id,w.surname||w.firstname worker_name,w.midlename,w.Ibname from Worker w where w.Ibname = '" + login + "' and w.Working = 1 and w.IN_REQUEST=1 and PWD='"+passwd+"'", function (err, result) {

                var worker;
                worker = JSON.stringify(result);
                var hash = md5(result.SURNAME + result.FIRSTNAME + cur_date);
                client.set(hash, worker);
                client.get(hash, function (err, res) {
                    console.log(res);
                });
                db.detach();

                if (result.length > 0) {
                    success = true;
                    error_code = 0;
                    error = "";
                } else {
                    success = false;
                    error_code = 1;
                    error = "not found user";

                }

                var answer = {
                    "success": success,
                    "error_code": error_code,
                    "error": error,
                    "hash": hash,
                    "data": result
                }
                res.setHeader('content-type', 'text/json');
                res.send(answer);
            });

        });
        break;
	

    case 'update_password' :
     try{
           var hash_key = result.hash_key;
           var data = JSON.parse(result.data);
           var pwd = data.pwd;

 client.get(hash_key, function (err, reply) {
  if (err) throw new Error('Smth wrong');
                 var login = JSON.parse(reply);
 if (!(util.isNull(reply))) {
                login = login[0].IBNAME;
                Firebird.attach(options, function (err, db) {
                        if (err) throw err;

                        console.log("UPDATE COMMAND");
                        // db = DATABASE
                        db.query(' UPDATE WORKER rg SET PWD=? WHERE   ibname=?', [pwd,login], function (err, result) {

                            db.detach();
                            res.status(200);
                            var answer =
                                {
                                    "success": true,
                                    "error": "",
                                    "error_code": 1,
                                    "data": {"succes": true}
                                }
                            res.setHeader('content-type', 'text/json');
                            res.send(answer);
                        });

                    });

            }else {
                    res.setHeader('content-type', 'text/json');
                    var answer =
                        {
                            "success": false,
                            "error": "",
                            "error_code": 1,
                            "data": ""
                        }
                    res.send(answer);
                }


 });

       } catch(err) {return next(err);}
       break

	case 'get_request_repeat' :
	console.log("get_request_repeat");
       try{
		   var hash_key = result.hash_key;

            client.get(hash_key, function (err, reply) {

                if (err) throw new Error('Smth wrong');
				 var login = JSON.parse(reply);

                if (!(util.isNull(reply))) {
				login = login[0].IBNAME;
				 Firebird.attach(options, function (err, db) {
                        if (err) throw err;

						console.log("UPDATE COMMAND");
                        // db = DATABASE
                        db.query(' UPDATE REQUEST_GIVED rg SET sended=0 WHERE   ibname=? and rg.date_sended BETWEEN (current_date-1) and (current_date+1)', [login], function (err, result) {

                            db.detach();
                            res.status(200);
                            var answer =
                                {
                                    "success": true,
                                    "error": "",
                                    "error_code": 1,
                                    "data": result
                                }
                            res.setHeader('content-type', 'text/json');
                            res.send(answer);
                        });

                    });
				
										}else {
                    res.setHeader('content-type', 'text/json');
                    var answer =
                        {
                            "success": false,
                            "error": "",
                            "error_code": 1,
                            "data": ""
                        }
                    res.send(answer);
                }
				
																
		 });// redis end

        } catch (err) {
            return next(err);
        }
		
        break;
		
		
    case  'get_request_list' :



        try {

           /* var req_sql = fs.readFileSync('./db/sql/request.sql', 'utf8');*/


            var hash_key = result.hash_key;

            console.log("Запрашиваю заявки для "+hash_key);

            client.get(hash_key, function (err, reply) {

                if (err) throw new Error('Smth wrong');

                var login = JSON.parse(reply);

                if (!(util.isNull(reply))) {
                    login = login[0].IBNAME;

                    Firebird.attach(options, function (err, db) {
                        if (err) throw err;

                         console.log("Запрашиваю заявки из базы firebird для "+hash_key);   
                        // db = DATABASE
                        db.query('SELECT * FROM GET_REQUEST(?)', [login], function (err, result) {

                            db.detach();
                            res.status(200);
                            var answer =
                                {
                                    "success": true,
                                    "error": "",
                                    "error_code": 1,
                                    "data": result
                                }

                             console.log("Отдаю результат "+result);   
                            res.setHeader('content-type', 'text/json');
                            res.send(answer);
                        });

                    });

/*var result = '{"success":true,"error":"","error_code":1,"data":[{"RQ_ID":187536,"W_TIME":0,"TIME_TO":"2020-07-21","RT_NAME":"Интернет новый","TPL_CONTENT":"","R_CONTENT":" ","NOTICE":"","RQ_TIME_FROM":null,"RQ_TIME_TO":null,"REQ_RESULT":3,"STREET":"2","HOUSE_NO":"1","ADRESS":"2 1 9","FLAT_NO":"9","CUSTOMER_ID":26987,"SUBAR":"","ACCOUNT":"38237","FIO":"Кабельное Вася Василевич","SERV_NAME":"","BALANCE":1350,"TARIF_MONTH":0,"IP":"10.77.9.114","PORT":"","PHONE":" ","SECRET":"beb05dc3","ADDED_BY":"SYSDBA","ADDED_ON":"2020-07-20 17:52:25.2970"}]}';
 console.log("Отдаю результат "+JSON.parse(result));
  res.status(200);
                            var answer =
                                {
                                    "success": true,
                                    "error": "",
                                    "error_code": 1,
                                    "data": JSON.parse(result)
                                }



                            res.setHeader('content-type', 'text/json');
                            res.send(answer);*/


                } else {
                    res.setHeader('content-type', 'text/json');
                    var answer =
                        {
                            "success": false,
                            "error": "",
                            "error_code": 1,
                            "data": ""
                        }
                    res.send(answer);
                }
            });// redis end

        } catch (err) {
            return next(err);
        }
        break;

    case  'set_requested_success' :// Получение заявок которые принял клиент
        var hash_key = result.hash_key;
		
	console.log("Клиент говорит какие заявки он получил");
		 client.get(hash_key, function (err, reply) {
		 console.log(hash_key);
		console.log('reply redis '+reply);
		var login = JSON.parse(reply);

                if (!(util.isNull(reply))) {
                    login = login[0].IBNAME;}
		
		
        var request_list = JSON.parse(result.data);

        console.log(result.data);

        Firebird.attach(options, function (err, db) {
            if (err) throw err;


// перебор элементов массива
            request_list.forEach(function (item, i, arr) {
                console.log(item.req_id);
				console.log(login);
				 db.query("UPDATE REQUEST_GIVED  SET sended=1 , date_sended=CURRENT_TIMESTAMP WHERE req_id=? and IBNAME=? and REQ_STATUS=?", [item.req_id,login,item.status], function (err, result) {
				
              /*  db.query("UPDATE request SET SENDED=1,DELIVER_DATE=CURRENT_TIMESTAMP WHERE rq_id=?", [item.req_id,], function (err, result) {  */

                    console.log(JSON.stringify(result));
                });

            });
//console.log(arr_reqid.toString());
            db.detach();//Отключение от базы Firebird
        });

                   });

        var answer =
            {
                "success": true,
                "error": "",
                "error_code": 1,
                "data": ""
            }

        res.send(answer);

//set_requested_success end

        break;
    case 'get_customer_info':// получение информации о абоненте из бд firebird
        var hash_key = result.hash_key;
        var customer_id = result.customer_id;

        client.get(hash_key, function (err, reply) {
            var json_str = JSON.parse(reply);
            if (!(util.isNull(reply))) {
                login = json_str[0].IBNAME;
                Firebird.attach(options, function (err, db) {

                    db.query("SELECT * FROM CUSTOMER WHERE CUSTOMER_ID=?", [customer_id], function (err, result) {

                        var answer =
                            {
                                "success": true,
                                "error": "",
                                "error_code": 1,
                                "data": result
                            };
                        res.send(answer);
                    });
                    db.detach();//Отключение от базы Firebird

                });// firebird end
            }

        });

        break;


    case 'set_pendind_result' : // Записать отложенные заявки
        var hash_key = result.hash_key;
        var request_list = JSON.parse(result.data);
        console.log(result);


        var success = false;
        var error_code = 0;
        var error = "";
       var arr_req = new Array();

        Firebird.attach(options, function (err, db) {


            db.transaction(Firebird.ISOLATION_READ_COMMITED, function (err, transaction) {

                request_list.forEach(function (item, i, arr) {

                    console.log(item.req_id);


                    transaction.query("UPDATE REQUEST SET REQ_RESULT = ?,RQ_EXEC_TIME = CURRENT_TIMESTAMP, RQ_DEFECT =?  WHERE RQ_ID=?", [item.req_result, item.description, item.req_id], function (err, result) {

                        if (err) {
                            transaction.rollback();
                            success = false;
                            error_code = 1;
                            console.log("transaction is rollback "+err);
                        }else
                        {

                            arr_req.push({"req_id": item.req_id});
                            var JPG =  new Buffer(item.foto, "base64");
                            db.query("INSERT INTO REQUEST_PHOTOS(RQ_ID,JPG,NOTICE,ADDED_BY,ADDED_ON,HOUSE_ID) VALUES(?,?,?,?,CURRENT_TIMESTAMP ,?)",[item.req_id,JPG,"","TEST",123],function (err, result) {

                                console.log("foto added to dbase");

                            })


                        }

                        if((request_list.length-1)==i)
                        {
                            transaction.commit(function (err) {

                                if (err) {
                                    transaction.rollback();
                                    success = false;
                                    error_code = 1;
                                    console.log("transaction is rollback "+err);
                                } else {
                                    success = true;
                                    error_code = 0;
                                    db.detach();
                                    console.log("transaction is success");
                                    console.log(JSON.stringify(arr_req));
                                }

                                var answer =
                                    {
                                        "success": success,
                                        "error_code": error_code,
                                        "error" : "",
                                        "data": JSON.stringify(arr_req)
                                    };

                                res.send(answer);


                            });
                        }



                    });




                });// foreach





            });// transaction zone

            //  db.query("UPDATE REQUEST SET REQ_RESULT = ?,RQ_EXEC_TIME = CURRENT_TIMESTAMP, RQ_DEFECT =?  WHERE REQ_ID=?" , [item.req_result,item.description,item.req_id], function (err, result) {
           db.detach();

        });

        //console.log(JSON.stringify(arr));


        break;
    case 'set_request_result' :// Записать статус заявки -  2- выполнено 3- отменена 4- невозможно
        var hash_key = result.hash_key;
        var data = JSON.parse(result.data);
        var req_id = data.req_id;
        var status = data.req_result;
        var description = data.description;

        console.log(result);
        console.log(data.req_id);

        var success =false;
        var error_code = 0;
        var error = "";
        var data_res = null;

        if(data.foto!=null){
        var JPG =  new Buffer(data.foto, "base64");}
        else {JPG = null;}

        Firebird.attach(options, function (err, db) {


        if(description.length>0) {

            db.query("UPDATE REQUEST SET REQ_RESULT = ?,RQ_EXEC_TIME = CURRENT_TIMESTAMP, RQ_DEFECT =?  WHERE RQ_ID=?", [status, description, req_id], function (err, result) {

                if (err) {
                    success = false;
                    error_code = 1;
                    error = "";
                } else {
                    success = true;
                    error_code = 0;
                    error = "";
                }


                var answer =
                    {
                        "success": success,
                        "error_code": error_code,
                        "error": error,
                        "req_id": req_id,
                        "data": data
                    };


                res.send(answer);


            });}
        else
        {

            db.query("UPDATE REQUEST SET REQ_RESULT = ?,RQ_EXEC_TIME = CURRENT_TIMESTAMP  WHERE RQ_ID=?", [status, req_id], function (err, result) {

                if (err) {
                    success = false;
                    error_code = 1;
                    error = "";
                } else {
                    success = true;
                    error_code = 0;
                    error = "";
                }


                var answer =
                    {
                        "success": success,
                        "error_code": error_code,
                        "error": error,
                        "req_id": req_id,
                        "data": data
                    };


                res.send(answer);


            });

        }

            if (JPG != null) {

            db.query("INSERT INTO REQUEST_PHOTOS(RQ_ID,JPG,NOTICE,ADDED_BY,ADDED_ON,HOUSE_ID) VALUES(?,?,?,?,CURRENT_TIMESTAMP ,?)", [req_id, JPG, "", "TEST", 123], function (err, result) {

                console.log("foto added to dbase");

            });
        }


            db.detach();

        });



        break;

    default :

        var answer =
            {
                "success": false,
                "error": "not found",
                "error_code": 1,
                "data": ""
            };
        res.setHeader("content-type", "text/json");
        res.send(answer);


}
});
//*****




client.on("error", function(err){
 console.log("Error" + err);
});

client.on("connect", function(con){
 console.log("Connected to Redis");
 
// client.quit();
});



app.get('/:id',function(req,res){

  res.send(req.params.id);
})

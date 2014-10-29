//const i32 a = 1
//const i32 b = 2
//const string c = "helloworld"
//const list<string> d = "test"
//const map<string, string> e = {"hello" : "world", "dd" : "ee"}

//struct f{
//    1:i32 a,
//    2:i32 b,
//    3:string c,
//   4:list<string> d=["ceshi"],
//    5:map<string,string> e = {"hello":"world"},
//}
//exception Exception{
//    1:i32 what;
//    2:string where;
//}

service CrawlerThrift{
    //string ping() throws (1:Exception e)
    i32 ping(1:string servicename)
    void logo (1:string servicename 2: i32 level 3: string msg)
    i32 start(1:string servicename)
    i32 stop(1:string servicename)
    i32 pause(1:string servicename)
    i32 resume(1:string servicename)
    string get_running_info (1:string servicename)
}

library(RMySQL)
library(dplyr)
library(httr)
library(readr)
library(lubridate)

#########OPTIONS##########
options(stringsAsFactors = F)
######ERROR LOG#########
zz <- file(paste0("/home/knimeuser/Documents/logs/clicks_40k_",Sys.Date(),".Rout"), open="wt")
sink(zz, type="message")
##########CONSTANTS##########
clickhouse_cnf<-read_csv("/home/knimeuser/Documents/config/clickhouse.csv")
#clickhouse_cnf<-read_csv("/Users/user/Documents/clickhouse.csv")
CLICKS_AMOUNT<-30000
PREVIOUS_HOUR<-Sys.time()-hours(1)
PREVIOUS_HOUR_DATE<-date(PREVIOUS_HOUR)
TODAY<-Sys.Date()
YESTERDAY<-TODAY-1
CHANNEL<-"feed_to_feed_cr_notif"
USERNAME<-"Your Doctor"
COLOR<-"#ff0000"
ICON_EMOJI<-":crying_cat_face:"
INCOMING_WEBHOOK<-"https://hooks.slack.com/services/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"

#####FUNCTIONS######
# get_list<-function(v){
#     l<-paste0(as.character(v), collapse=",")
#     return(l)
# }
BODY<-"SELECT DISTINCT iduser as id FROM feed_to_feed_settings WHERE active=1"
mysql_query<-function(body){
    mydb = dbConnect(MySQL(), group='bi',default.file="/home/knimeuser/Documents/config/mysql.cnf")
    #mydb = dbConnect(MySQL(), group='bi',default.file="/Users/user/Documents/mysql.cnf") 
    res = dbSendQuery(mydb, body)
    df=fetch(res,n=-1)
    dbDisconnect(mydb)
    return(df$id)
}
post_to_slack<-function(message, USERNAME){
    resp <- POST(url = INCOMING_WEBHOOK, encode = "form", 
                 add_headers(`Content-Type` = "application/x-www-form-urlencoded", 
                             Accept = "*/*"), body = URLencode(sprintf("payload={\"channel\": \"%s\", \"username\": \"%s\", \"attachments\": [{\"color\":\"%s\", \"text\": \"%s\", \"icon_emoji\":\"%s\"}]}", 
                                                                       CHANNEL, USERNAME, COLOR, message, ICON_EMOJI)))
}
get_data<-function(body){
    query <- POST(clickhouse_cnf$post_url[[1]], 
               body = body,
               config(username=clickhouse_cnf$user[[1]],password=clickhouse_cnf$password[[1]],httpauth=1))
    data<-content(query)
    return(data)
}
construct_body<-function(x, flag=1, limit=CLICKS_AMOUNT, date=PREVIOUS_HOUR_DATE){
    if(date==TODAY & flag==1){
        table_clicks<-paste0("stat.daily_clicks_", format(date, format='%Y_%m_%d'))
        table_leads<-paste0("stat.daily_leads_", format(date, format='%Y_%m_%d'))
    }else{
        table_clicks<-"stat.offertrack_ct_clicks"
        table_leads<-"stat.offertrack_ct_leads"
    }
    body<-paste0("
             SELECT id, ds, aid, valid, cpl 
             FROM (
                   SELECT id,ds, aid, valid
                   FROM ",table_clicks,"
                   WHERE aid=",id_vector[[x]]," AND toDate(datecomp)='",date,"' AND valid=1
             ) ANY LEFT JOIN (
                   SELECT id, aid, cpl
                   FROM ",table_leads,"
                   WHERE aid=",id_vector[[x]]," AND toDate(datecomp)='",date,"' AND cpl IN (1,4)
             )
             USING id, aid
             ORDER BY ds DESC
             LIMIT ",limit,"
             FORMAT CSVWithNames
             ")
}
#####CODE#####
id_vector<-mysql_query(body=BODY)
aid_list<-vector()
no_clicks<-vector()
for (i in 1:length(id_vector)){
    body<-construct_body(i)
    data<-get_data(body)
    if (nrow(data)==0){
        body<-construct_body(i, flag=2)
        data<-get_data(body)
        if (nrow(data)==0){
            no_clicks<-c(no_clicks, id_vector[[i]])
            next}
    }
    leads<-sum(data$cpl %in% c(1,4))
    if (leads==1){next}
    if (leads==0 & nrow(data)<CLICKS_AMOUNT){
        new_limits=CLICKS_AMOUNT-nrow(data)
        new_body<-construct_body(i, flag = 2, limit = new_limits, date = YESTERDAY)
        new_data<-get_data(new_body)
        if((nrow(data)+nrow(new_data))<CLICKS_AMOUNT){next}
        new_leads<-sum(new_data$cpl %in% c(1,4))
        if (new_leads==0){
            aid_list<-c(aid_list, id_vector[[i]])
        }
    }else if(leads==0 & nrow(data)==CLICKS_AMOUNT){
        aid_list<-c(aid_list, id_vector[[i]])
    }
}
aid_char_list<-paste0(as.character(aid_list), collapse = ",\n")
message<-paste0("Advertisers that did not get valid leads in last ",as.character(CLICKS_AMOUNT)," clicks:\n", aid_char_list)
post_to_slack(message = message, USERNAME = USERNAME)

sink(type="message")
close(zz)
//
// Created by matteo on 09/04/2021.
//

#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <json/json.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>
#include <csignal>

volatile std::atomic<bool> keep_running = ATOMIC_FLAG_INIT;

extern "C" void signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        keep_running.store(false, std::memory_order_acq_rel);
    }
}


void print_json(const Json::Value &value) {
    Json::StreamWriterBuilder builder;
//    builder.settings_["indentation"] = "";
    const std::string serialized = Json::writeString(builder, value);
    std::cout << serialized << std::endl;
}

void print_table_value(const AmqpClient::TableValue &value) {
    switch (value.GetType()) {
        case AmqpClient::TableValue::VT_array:
            std::cout << '[';
            for (auto const &elem : value.GetArray()) {
                print_table_value(elem);
                std::cout << ',';
            }
            std::cout << ']';
            break;
        case AmqpClient::TableValue::VT_bool:
            std::cout << (value.GetBool() ? "true" : "false");
            break;
        case AmqpClient::TableValue::VT_double:
        case AmqpClient::TableValue::VT_float:
            std::cout << value.GetReal();
            break;
        case AmqpClient::TableValue::VT_string:
            std::cout << '"' << value.GetString() << '"';
            break;
        case AmqpClient::TableValue::VT_table:
            std::cout << '{';
            for (auto const &v : value.GetTable()) {
                std::cout << v.first << ':';
                print_table_value(v.second);
            }
            std::cout << '}';
            break;
        case AmqpClient::TableValue::VT_timestamp:
            std::cout << value.GetTimestamp();
            break;
        case AmqpClient::TableValue::VT_int16:
        case AmqpClient::TableValue::VT_int32:
        case AmqpClient::TableValue::VT_int64:
        case AmqpClient::TableValue::VT_int8:
        case AmqpClient::TableValue::VT_uint16:
        case AmqpClient::TableValue::VT_uint32:
        case AmqpClient::TableValue::VT_uint8:
            std::cout << value.GetInteger();
            break;
        case AmqpClient::TableValue::VT_void:
            std::cout << "null";
            break;
    }
}

//TODO send hearthbeat:
// {'hostname': 'celery@tenebra', 'utcoffset': -2, 'pid': 29142, 'clock': 717, 'freq': 2.0, 'active': 0, 'processed': 4, 'loadavg': (1.64, 1.83, 2.12), 'sw_ident': 'py-celery', 'sw_ver': '5.0.5', 'sw_sys': 'Linux', 'timestamp': 1618264351.0156732, 'type': 'worker-heartbeat'}

void reply(AmqpClient::Channel::ptr_t channel, const std::string &reply_to, const std::string &task_id, int result) {
    const std::string routing_key = reply_to;
    const std::string correlation_id = task_id;
    const char *state = "SUCCESS";

    Json::Value msg;
    msg["task_id"] = task_id;
    msg["status"] = state;
    msg["result"] = result;
    msg["traceback"] = Json::Value::null;
    msg["children"] = Json::Value(Json::ValueType::arrayValue);

    Json::StreamWriterBuilder builder;
    builder.settings_["indentation"] = "";
    const std::string serialized = Json::writeString(builder, msg);

    auto a_msg = AmqpClient::BasicMessage::Create(serialized);
    a_msg->ContentType("application/json");
    a_msg->ContentEncoding("utf-8");
//    a_msg->HeaderTable({
//                             {"id", "3149beef-be66-4b0e-ba47-2fc46e4edac3"},
//                             {"task", "test_worker.add"}
//                     });

    channel->BasicPublish("", routing_key, a_msg);
}

int main(int argc, char* argv[])
{
    keep_running.store(true, std::memory_order_acq_rel);
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    auto options = AmqpClient::Channel::OpenOpts();
    options.host = "localhost";
    options.auth = AmqpClient::Channel::OpenOpts::BasicAuth("guest", "guest");
    auto channel = AmqpClient::Channel::Open(options);

    auto consumer_tag = channel->BasicConsume(
            "celery",
            "", // empty string, the library will generate this for us
            true,
            false, //acknowledge manually
            false, //allow other workers
            1 //no buffering
            );
    std::cout << "Started worker with tag: " << consumer_tag << std::endl;

    while (keep_running) {
        AmqpClient::Envelope::ptr_t envelope;
        auto message_received = channel->BasicConsumeMessage(
                consumer_tag,
                envelope,
                1000);
        if (!message_received)
            continue;
        auto message = envelope->Message();

        auto reply_to = message->ReplyTo();

        const std::string &body_string = message->Body();
        std::cout << "received task: " << body_string << std::endl;
        Json::Value body;
        JSONCPP_STRING err;
        Json::CharReaderBuilder builder;
        const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        const char *start = body_string.c_str();
        const char *end = start + body_string.length() - 1;
        bool error = reader->parse(start, end, &body, &err);
        if (error) {
            std::cerr << "error parsing json" << err << std::endl;
            channel->BasicReject(envelope, false);
            channel->BasicCancel(consumer_tag);
            return -1;
        }

//        std::cout << "this is the parsed body: " << std::endl;
//        print_json(body);
        const int x = body[0][0].asInt();
        const int y = body[0][1].asInt();

        const std::string &task_id = message->HeaderTable().at("id").GetString();

        std::cout << "working on task " << task_id << std::endl << x << " + " << y << std::endl;
//        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        std::cout << "working done " << std::endl;
        channel->BasicAck(envelope);

        reply(channel, reply_to, task_id, x + y);
    }

    channel->BasicCancel(consumer_tag);

    return 0;



}
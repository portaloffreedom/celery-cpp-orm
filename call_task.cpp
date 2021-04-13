//
// Created by matteo on 09/04/2021.
//

#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <json/json.h>
#include <iostream>

int main(int argc, char* argv[])
{
    auto options = AmqpClient::Channel::OpenOpts();
    options.host = "localhost";
    options.auth = AmqpClient::Channel::OpenOpts::BasicAuth("guest", "guest");
    auto channel = AmqpClient::Channel::Open(options);

    Json::Value root;
    Json::Value args;
    Json::Value kwargs;
    Json::Value other;

    args.append(4);
    args.append(5);

    root.append(args);
    root.append(Json::Value(Json::ValueType::objectValue));
    root.append(Json::Value(Json::ValueType::objectValue));

    Json::StreamWriterBuilder builder;
    builder.settings_["indentation"] = "";
//    const std::string serialized2 = Json::writeString(builder, builder.settings_);
//    std::cout << serialized2 << std::endl;
    const std::string serialized = Json::writeString(builder, root);
    std::cout << serialized << std::endl;

    auto msg = AmqpClient::BasicMessage::Create(serialized);
    msg->ContentType("application/json");
    msg->ContentEncoding("utf-8");
    msg->HeaderTable({
                             {"id", "3149beef-be66-4b0e-ba47-2fc46e4edac3"},
                             {"task", "test_worker.add"}
    });

    channel->BasicPublish("celery", "celery", msg);
}
/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include <iostream>
#include <memory>
#include <chrono>

#include <kudu/client/client.h>
#include <kudu/client/row_result.h>

#include <tcl/tcl.h>

void assertOk(kudu::Status status) {
    if (!status.ok()) {
        std::cerr << "An error occurred while inserting: " << status.message().ToString() << std::endl;
        std::terminate();
    }
}

void createSchema(kudu::client::KuduClient& client) {
    kudu::client::KuduSchema schema;
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(client.NewTableCreator());

    kudu::client::KuduSchemaBuilder schemaBuilder;
    auto col = schemaBuilder.AddColumn("mykey");
    col->Type(kudu::client::KuduColumnSchema::INT64);
    col->NotNull();
    col = schemaBuilder.AddColumn("textcol1");
    col->Type(kudu::client::KuduColumnSchema::STRING);
    col->NotNull();
    col = schemaBuilder.AddColumn("textcol2");
    col->Type(kudu::client::KuduColumnSchema::STRING);
    col->NotNull();
    col = schemaBuilder.AddColumn("textcol3");
    col->Type(kudu::client::KuduColumnSchema::STRING);
    col->NotNull();
    col = schemaBuilder.AddColumn("salary");
    col->Type(kudu::client::KuduColumnSchema::FLOAT);
    col->NotNull();
    schemaBuilder.SetPrimaryKey({ "mykey" });
    assertOk(schemaBuilder.Build(&schema));

    tableCreator->table_name("test");
    tableCreator->schema(&schema);
    assertOk(tableCreator->Create());
}

int populateCmd(ClientData data, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) {
    auto start = std::chrono::system_clock::now();
    auto& client = *reinterpret_cast<std::tr1::shared_ptr<kudu::client::KuduClient>*>(data);
    createSchema(*client);
    std::tr1::shared_ptr<kudu::client::KuduTable> table;
    client->OpenTable("test", &table);
    auto session = client->NewSession();
    session->SetTimeoutMillis(60000);
    assertOk(session->SetFlushMode(kudu::client::KuduSession::FlushMode::MANUAL_FLUSH));
    kudu::Slice textSlice("Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, conset");
    for (int i = 0; i < 120000; ++i) {
        for (int j = 0; j < 100; ++j) {
            auto insert = table->NewInsert();
            auto row = insert->mutable_row();
            assertOk(row->SetInt64("mykey", i * 1000 + j));
            assertOk(row->SetString("textcol1", textSlice));
            assertOk(row->SetString("textcol2", textSlice));
            assertOk(row->SetString("textcol3", textSlice));
            assertOk(row->SetFloat("salary", 0.05));
            assertOk(session->Apply(insert));
        }
        std::cout << "Populated " << (i+1)*1000 << " rows\n";
        assertOk(session->Flush());
    }
    std::cout << "Command completed in " << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start).count() << " s\n";
    return TCL_OK;
}

int scanCmd(ClientData data, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) {
    auto& client = *reinterpret_cast<std::tr1::shared_ptr<kudu::client::KuduClient>*>(data);
    std::tr1::shared_ptr<kudu::client::KuduTable> table;
    assertOk(client->OpenTable("test", &table));
    auto start = std::chrono::system_clock::now();
    kudu::client::KuduScanner scanner(table.get());
    assertOk(scanner.SetProjectedColumnNames({"salary"}));
    assertOk(scanner.Open());
    std::vector<kudu::client::KuduRowResult> rows;
    kudu::Slice salary("salary");
    float res = 0.0f;
    long cnt = 0;
    while (scanner.HasMoreRows()) {
        rows.clear();
        scanner.NextBatch(&rows);
        for (const auto& row : rows) {
            float r;
            assertOk(row.GetFloat(salary, &r));
            res += r;
            ++cnt;
        }
    }
    std::cout << "SUM: " << res << std::endl;
    std::cout << "Scanned " << cnt << " rows\n";
    std::cout << "Scan took " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start).count() << "ms to complete\n";
    return TCL_OK;
}

int getCmd(ClientData data, Tcl_Interp* interp, int objc, Tcl_Obj* const objv[]) {
    if (objc != 2) {
        std::cerr << "USAGE: getRow primarykey" << std::endl;
    }
    auto& client = *reinterpret_cast<std::tr1::shared_ptr<kudu::client::KuduClient>*>(data);
    std::tr1::shared_ptr<kudu::client::KuduTable> table;
    assertOk(client->OpenTable("test", &table));
    int key;
    if (Tcl_GetIntFromObj(interp, objv[1], &key) != TCL_OK) {
        return TCL_ERROR;
    }
    auto begin = std::chrono::system_clock::now();
    kudu::client::KuduScanner scanner(table.get());
    assertOk(scanner.AddConjunctPredicate(table->NewComparisonPredicate("mykey", kudu::client::KuduPredicate::EQUAL, kudu::client::KuduValue::FromInt(key))));
    assertOk(scanner.Open());
    std::vector<kudu::client::KuduRowResult> rows;
    while (scanner.HasMoreRows()) {
        scanner.NextBatch(&rows);
        for (auto& row : rows) {
            float f;
            assertOk(row.GetFloat("salary", &f));
            std::cout << f << std::endl;
        }
    }
    auto t = std::chrono::system_clock::now() - begin;
    std::cout << "Fetched in " << std::chrono::duration_cast<std::chrono::microseconds>(t).count() << "us\n";
    return TCL_OK;
}

int connectCmd(ClientData data, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) {
    auto client = reinterpret_cast<std::tr1::shared_ptr<kudu::client::KuduClient>*>(data);
    kudu::client::KuduClientBuilder clientBuilder;
    clientBuilder.add_master_server_addr("euler09");
    clientBuilder.Build(client);

    return TCL_OK;
}

void deleteClient(ClientData data) {
    delete reinterpret_cast<std::tr1::shared_ptr<kudu::client::KuduClient>*>(data);
}

int initTcl(Tcl_Interp* interp) {
    std::tr1::shared_ptr<kudu::client::KuduClient>* clientPtr = new std::tr1::shared_ptr<kudu::client::KuduClient>();
    Tcl_CreateObjCommand(interp, "connect", &connectCmd, clientPtr, &deleteClient);
    Tcl_CreateObjCommand(interp, "populate", &populateCmd, clientPtr, nullptr);
    Tcl_CreateObjCommand(interp, "scan", &scanCmd, clientPtr, nullptr);
    Tcl_CreateObjCommand(interp, "getRow", &getCmd, clientPtr, nullptr);
    return TCL_OK;
}

int main(int argc, char** argv) {
    Tcl_Interp* interp = Tcl_CreateInterp();
    Tcl_Init(interp);
    Tcl_MainEx(argc, argv, &initTcl, interp);
    Tcl_Finalize();
    return 0;
}

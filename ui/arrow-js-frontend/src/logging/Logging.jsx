import React, { useState } from "react";
import "../App.css";
import { useLogging } from "../context/LoggingContext";
import { useQueryManagement } from "../hooks/useQueryManagement";
import { useConnectionForm } from "../hooks/useConnectionForm";
import { useSessionManagement } from "../hooks/useSessionManagement";
import ConnectionPanel from "../components/logging/ConnectionPanel";
import QueryRow from "../components/logging/QueryRow";
import PopupMessage from "../components/utils/PopupMessage";
import SearchTable from "../components/logging/SearchTable";

const Logging = () => {
    const {
        executeQuery,
        login,
        logout,
        cancelQuery,
        saveSession,
        loadSession,
        restoreSession,
        connectionInfo,
    } = useLogging();

    const [activeTab, setActiveTab] = useState("analytics"); // "analytics" | "search"

    const [popup, setPopup] = useState({
        message: "",
        type: "",
        visible: false,
    });

    const showPopup = (message, type = "success") => {
        setPopup({
            message,
            type,
            visible: true,
        });
    };

    const [showConnection, setShowConnection] = useState(true);

    // Connection form management
    const connectionForm = useConnectionForm(login, logout, connectionInfo);

    // Query management
    const queryManagement = useQueryManagement(
        executeQuery,
        cancelQuery,
        connectionForm.isConnected,
        connectionForm.connection
    );

    // Session management
    const sessionManagement = useSessionManagement(
        saveSession,
        loadSession,
        restoreSession,
        queryManagement.rows,
        connectionForm.populateConnectionData,
        queryManagement.restoreRows,
        showPopup
    );

    // Handle logout with query reset
    const handleLogout = () => {
        connectionForm.handleLogout();
        queryManagement.resetRows();
    };

    return (
        <div className="relative min-h-screen bg-gradient-to-br from-gray-50 to-gray-200 p-10 space-y-10">
            {/* Connection Panel */}
            <ConnectionPanel
                showConnection={showConnection}
                setShowConnection={setShowConnection}
                isConnected={connectionForm.isConnected}
                register={connectionForm.register}
                handleSubmit={connectionForm.handleSubmit}
                onSubmit={connectionForm.onSubmit}
                errors={connectionForm.errors}
                isSubmitted={connectionForm.isSubmitted}
                isSubmitting={connectionForm.isSubmitting}
                loginError={connectionForm.loginError}
                handleLogout={handleLogout}
                showAdvanced={connectionForm.showAdvanced}
                setShowAdvanced={connectionForm.setShowAdvanced}
                claims={connectionForm.claims}
                addClaim={connectionForm.addClaim}
                removeClaim={connectionForm.removeClaim}
                updateClaim={connectionForm.updateClaim}
                fileInputRef={sessionManagement.fileInputRef}
                handleSaveSession={sessionManagement.handleSaveSession}
                handleOpenSession={sessionManagement.handleOpenSession}
                openFileDialog={sessionManagement.openFileDialog}
            />

            {/* Tabs */}
            <div className="flex gap-4 border-b border-gray-400 pb-2 ml-5 overflow-hidden">
                <button
                    onClick={() => setActiveTab("analytics")}
                    className={` px-4 pt-2 pb-3 font-semibold rounded-t-md border border-b-0 cursor-pointer transition-all duration-300 transform ${activeTab === "analytics" ? "bg-gray-300 border-gray-600 translate-y-4" : "bg-gray-200 border-gray-200 text-gray-600 hover:text-gray-900 translate-y-3"}`}
                >
                    Analytics
                </button>

                <button
                    onClick={() => setActiveTab("search")}
                    className={` px-4 pt-2 pb-3 font-semibold rounded-t-md border border-b-0 transition-all duration-300 transform cursor-pointer ${activeTab === "search" ? "bg-gray-300 border-gray-600 translate-y-4" : "bg-gray-200 border-gray-200 text-gray-600 hover:text-gray-900 translate-y-3"}`}
                >
                    Search
                </button>
            </div>

            {activeTab === "analytics" && (
                <>
                    {/* Query Rows */}
                    {queryManagement.rows.map((row) => (
                        <QueryRow
                            key={row.id}
                            row={row}
                            result={queryManagement.results[row.id]}
                            queryId={queryManagement.queryIds[row.id]}
                            isConnected={connectionForm.isConnected}
                            isCancelling={queryManagement.cancellingQueries[row.id]}
                            totalRows={queryManagement.rows.length}
                            updateRow={queryManagement.updateRow}
                            removeRow={queryManagement.removeRow}
                            handleRunQuery={queryManagement.handleRunQuery}
                            handleCancelQuery={queryManagement.handleCancelQuery}
                            clearRowLogs={queryManagement.clearRowLogs}
                        />
                    ))}

                    {/* Buttons */}
                    <div className="flex justify-evenly mt-10 gap-5">
                        <button
                            onClick={queryManagement.addRow}
                            className="bg-green-600 hover:bg-green-700 text-white font-semibold py-3 px-6 rounded-md"
                        >
                            Add New Query Row
                        </button>

                        <button
                            onClick={queryManagement.runAllQueries}
                            disabled={!connectionForm.isConnected || queryManagement.isRunningAll}
                            className="bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-md disabled:opacity-50"
                        >
                            {queryManagement.isRunningAll ? "Running..." : "Run Queries"}
                        </button>
                    </div>
                </>
            )}

            {activeTab === "search" && (
                <div className="">
                    <SearchTable
                        title="Search Logs"
                        data={[
                            { time: 1760342400000, value: 18, host: 'h1' },
                            { time: 1760432400000, value: 920000, host: 'h2' },
                            { time: 1760522400000, value: 310, host: 'h3' },
                            { time: 1760612400000, value: 150, host: 'h1' },
                            { time: 1760702400000, value: 24, host: 'h2' },
                            { time: 1760792400000, value: 870000, host: 'h3' },
                            { time: 1760882400000, value: 295, host: 'h4' },
                            { time: 1760972400000, value: 170, host: 'h1' },
                            { time: 1761033600000, value: 22, host: 'h2' },
                            { time: 1761123600000, value: 910000, host: 'h3' },
                            { time: 1761213600000, value: 280, host: 'h4' },
                            { time: 1761303600000, value: 160, host: 'h1' },
                            { time: 1761393600000, value: 26, host: 'h2' },
                            { time: 1761483600000, value: 940000, host: 'h3' }
                        ]}
                        loading={false}
                        error=""
                    />
                </div>
            )}

            <PopupMessage
                message={popup.message}
                type={popup.type}
                visible={popup.visible}
                onClose={() =>
                    setPopup({ message: "", type: "", visible: false })
                }
            />
        </div>
    );
};

export default Logging;
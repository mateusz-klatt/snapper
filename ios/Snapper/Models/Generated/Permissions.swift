// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

enum Permission: String, CaseIterable, Codable, Sendable {
    case readMarketData = "read:market_data"
    case readOrders = "read:orders"
    case createOrders = "create:orders"
    case cancelOrders = "cancel:orders"
    case readPositions = "read:positions"
    case managePositions = "manage:positions"
    case readStrategies = "read:strategies"
    case startStrategies = "start:strategies"
    case stopStrategies = "stop:strategies"
    case configureStrategies = "configure:strategies"
    case readSystemStatus = "read:system_status"
    case manageProcesses = "manage:processes"
    case configureSystem = "configure:system"
    case manageUsers = "manage:users"
    case readWalletCredentials = "read:wallet_credentials"
    case manageWalletCredentials = "manage:wallet_credentials"
    case manageScopeGrants = "manage:scope_grants"
    case impersonateOperator = "impersonate:operator"
}

let rolePermissions: [UserRole: [Permission]] = [
    .viewer: [.readMarketData, .readOrders, .readPositions, .readStrategies, .readSystemStatus],
    .operatorRole: [.cancelOrders, .createOrders, .managePositions, .manageProcesses, .readMarketData, .readOrders, .readPositions, .readStrategies, .readSystemStatus, .startStrategies, .stopStrategies],
    .admin: [.cancelOrders, .configureStrategies, .configureSystem, .createOrders, .impersonateOperator, .managePositions, .manageProcesses, .manageScopeGrants, .manageUsers, .manageWalletCredentials, .readMarketData, .readOrders, .readPositions, .readStrategies, .readSystemStatus, .readWalletCredentials, .startStrategies, .stopStrategies],
]

let resourceAccess: [String: [UserRole]] = [
    "overview": [.viewer, .operatorRole, .admin],
    "market": [.viewer, .operatorRole, .admin],
    "processes": [.operatorRole, .admin],
    "strategies": [.viewer, .operatorRole, .admin],
    "orders": [.viewer, .operatorRole, .admin],
    "positions": [.viewer, .operatorRole, .admin],
    "signals": [.viewer, .operatorRole, .admin],
    "health": [.viewer, .operatorRole, .admin],
    "admin": [.admin],
    "settings": [.admin],
]

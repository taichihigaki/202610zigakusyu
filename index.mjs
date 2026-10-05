import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand, PutCommand, DeleteCommand, GetCommand } from "@aws-sdk/lib-dynamodb";
import { ApiGatewayManagementApiClient, PostToConnectionCommand } from "@aws-sdk/client-apigatewaymanagementapi";

const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);

const CONNECTIONS_TABLE = "chat-connections";
const MESSAGES_TABLE = "chat-messages";
const USERS_TABLE = "chat-users";

const MASTER_NAME = "檜垣";
const MASTER_PASSWORD = "oitalove09";

export const handler = async (event) => {
  const routeKey = event.requestContext.routeKey;
  const connectionId = event.requestContext.connectionId;
  const domainName = event.requestContext.domainName;
  const stage = event.requestContext.stage;

  const apigw = new ApiGatewayManagementApiClient({
    endpoint: `https://${domainName}/${stage}`
  });

  const broadcast = async (data) => {
    const scanResult = await docClient.send(new ScanCommand({ TableName: CONNECTIONS_TABLE }));
    const connections = scanResult.Items || [];
    const promises = connections.map(async ({ connectionId: connId }) => {
      try {
        await apigw.send(new PostToConnectionCommand({ ConnectionId: connId, Data: JSON.stringify(data) }));
      } catch (e) {
        if (e.$metadata && e.$metadata.httpStatusCode === 410) {
          await docClient.send(new DeleteCommand({ TableName: CONNECTIONS_TABLE, Key: { connectionId: connId } }));
        }
      }
    });
    await Promise.all(promises);
  };

  if (routeKey === "$connect") {
    await docClient.send(new PutCommand({ TableName: CONNECTIONS_TABLE, Item: { connectionId } }));
    return { statusCode: 200, body: "Connected." };
  }

  if (routeKey === "$disconnect") {
    await docClient.send(new DeleteCommand({ TableName: CONNECTIONS_TABLE, Key: { connectionId } }));
    return { statusCode: 200, body: "Disconnected." };
  }

  if (routeKey === "sendmessage") {
    const body = JSON.parse(event.body);
    const { actionType, data } = body;

    // --- 初期化 & 履歴取得 ---
    if (actionType === "init") {
      const roomId = data?.roomId || "general";
      const userId = data?.userId;

      const historyResult = await docClient.send(new ScanCommand({ TableName: MESSAGES_TABLE }));
      const messages = (historyResult.Items || [])
        .filter(m => (m.roomId || "general") === roomId)
        .sort((a, b) => a.timestamp - b.timestamp);

      let userStatus = "free";
      if (userId) {
        const userRes = await docClient.send(new GetCommand({ TableName: USERS_TABLE, Key: { userId } }));
        if (userRes.Item) userStatus = userRes.Item.status || "free";
      }

      await apigw.send(new PostToConnectionCommand({
        ConnectionId: connectionId,
        Data: JSON.stringify({ type: "initResponse", data: { messages, userStatus } })
      }));
      return { statusCode: 200 };
    }

    // --- メッセージ送信 ---
    if (actionType === "chat") {
      if (data.username === MASTER_NAME && data.masterPassword !== MASTER_PASSWORD) {
        return { statusCode: 403, body: "この名前は使用できません。" };
      }

      const messageItem = {
        messageId: Date.now().toString() + "_" + Math.random().toString(36).substring(2, 7),
        roomId: data.roomId || "general",
        userId: data.userId,
        username: data.username,
        text: data.text,
        isPaid: data.isPaid || false,
        timestamp: Date.now()
      };

      await docClient.send(new PutCommand({ TableName: MESSAGES_TABLE, Item: messageItem }));
      await broadcast({ type: "chat", data: messageItem });
      return { statusCode: 200 };
    }

    // --- 有料会員申請 ---
    if (actionType === "requestPaid") {
      await docClient.send(new PutCommand({
        TableName: USERS_TABLE,
        Item: { userId: data.userId, username: data.username, status: "pending", requestedAt: Date.now() }
      }));

      const allUsersRes = await docClient.send(new ScanCommand({ TableName: USERS_TABLE }));
      const pendingUsers = (allUsersRes.Items || []).filter(u => u.status === "pending");

      await broadcast({ type: "userStatusChanged", data: { userId: data.userId, status: "pending", pendingUsers } });
      return { statusCode: 200 };
    }

    // --- マスター検証 & 承認/拒否 ---
    if (actionType === "verifyMaster" || actionType === "approveUser") {
      if (data.username !== MASTER_NAME || data.masterPassword !== MASTER_PASSWORD) {
        await apigw.send(new PostToConnectionCommand({
          ConnectionId: connectionId,
          Data: JSON.stringify({ type: "masterAuthResult", success: false, message: "マスター認証に失敗しました。" })
        }));
        return { statusCode: 403 };
      }

      if (actionType === "verifyMaster") {
        const allUsersRes = await docClient.send(new ScanCommand({ TableName: USERS_TABLE }));
        const pendingUsers = (allUsersRes.Items || []).filter(u => u.status === "pending");

        await apigw.send(new PostToConnectionCommand({
          ConnectionId: connectionId,
          Data: JSON.stringify({ type: "masterAuthResult", success: true, pendingUsers })
        }));
        return { statusCode: 200 };
      }

      if (actionType === "approveUser") {
        const newStatus = data.approve ? "paid" : "free";
        await docClient.send(new PutCommand({
          TableName: USERS_TABLE,
          Item: { userId: data.targetUserId, username: data.targetUsername, status: newStatus, updatedAt: Date.now() }
        }));

        const allUsersRes = await docClient.send(new ScanCommand({ TableName: USERS_TABLE }));
        const pendingUsers = (allUsersRes.Items || []).filter(u => u.status === "pending");

        await broadcast({ type: "userStatusChanged", data: { userId: data.targetUserId, status: newStatus, pendingUsers } });
        return { statusCode: 200 };
      }
    }

    // --- 送信取り消し ---
    if (actionType === "deleteMessage") {
      await docClient.send(new DeleteCommand({ TableName: MESSAGES_TABLE, Key: { messageId: data.messageId } }));
      await broadcast({ type: "deleteMessage", data: { messageId: data.messageId } });
      return { statusCode: 200 };
    }
  }

  return { statusCode: 400 };
};
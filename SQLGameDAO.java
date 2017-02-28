package server.DataAccessObject.GameDAO;

import org.apache.commons.lang3.SerializationUtils;
import server.database.Database;
import server.lists.CatanGame;

import java.io.IOException;
import java.rmi.ServerException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class SQLGameDAO implements IGameDAO
{

    @Override
    public boolean addGame(CatanGame game)
    {
        PreparedStatement stmt = null;
        ResultSet keyRS = null;
        try {
            String query = "INSERT INTO Game (GameID, Name, GameInfo) VALUES (?, ?, ?)";
            stmt = Database.getInstance().getConnection().prepareStatement(query);
            stmt.setString(1, "" + game.getGameID());
            stmt.setString(2, game.getName());
            byte[] data = SerializationUtils.serialize(game);
            stmt.setBytes(3, data);
            stmt.executeUpdate();
        } catch (SQLException | IOException e) {
            return false;
        } finally {
            Database.safeClose(stmt);
            Database.safeClose(keyRS);
        }
        return true;
    }

    @Override
    public boolean updateGame(CatanGame game) throws Exception
    {
        PreparedStatement stmt = null;
        try {
            String query = "UPDATE Game SET GameInfo = ? WHERE GameID = ?";
            stmt = Database.getInstance().getConnection().prepareStatement(query);
            byte[] data = SerializationUtils.serialize(game);
            stmt.setBytes(1, data);
            stmt.setInt(2, game.getGameID());
            if (stmt.executeUpdate() != 1) {
                throw new Exception("Could not update game");
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new Exception("Could not update game", e);
        } finally {
            Database.safeClose(stmt);
        }
        return true;
    }

    @Override
    public ArrayList<CatanGame> getGames() throws Exception
    {
        ArrayList<CatanGame> games = new ArrayList<CatanGame>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            String query = "SELECT GameInfo FROM Game";
            stmt = Database.getInstance().getConnection().prepareStatement(query);
            rs = stmt.executeQuery();
            while (rs.next()) {
                byte[] data = rs.getBytes(1);
                CatanGame game = SerializationUtils.deserialize(data);
                games.add(game);

            }
        } catch (SQLException | IOException e) {
            throw new Exception();
        } finally {
            Database.safeClose(rs);
            Database.safeClose(stmt);
        }
        return games;

    }

    @Override
    public void startTransaction() throws Exception
    {
        Database.getInstance().startTransaction();
    }

    @Override
    public void endTransaction() throws Exception
    {
        Database.getInstance().endTransaction(true);
    }

    @Override
    public void rollback()
    {
        try {
            Database.getInstance().endTransaction(false);
        } catch (ServerException e) {
            System.out.println("Rollback failed");
            e.printStackTrace();
        }
    }
}

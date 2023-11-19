package trocaBancoPostgres.migrar;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import trocaBancoPostgres.conexao.ConexaoAntiga;
import trocaBancoPostgres.conexao.ConexaoNova;

public class CFOP {

	public static void migrarTabela() {
		try (Connection remetenteConexao = ConexaoAntiga.obterConexao()) {

			String query = " SELECT id_natureza, desc_natureza, tipo_natureza, "
					+ "usuario_natureza, registro_natureza, id_referencianatureza, stnat\n"
					+ "FROM public.natureza where stnat =1;";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {
					int id = resultSet.getInt("id_referencianatureza");
					String descricao = resultSet.getString("desc_natureza");
					System.out.println("" + descricao);
					salvar(id, "", descricao);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void salvar(int idAntigo, String codigo, String descricao) {
		try (Connection novaConexao = ConexaoNova.obterConexao()) {
			String sql = "INSERT INTO CFOP (codigo, descricao, ativo,idAntigo) VALUES (?, ?, true,?)";

			try (PreparedStatement preparedStatement = novaConexao.prepareStatement(sql)) {

				preparedStatement.setString(1, codigo);
				preparedStatement.setString(2, descricao);
				preparedStatement.setInt(3, idAntigo);

				preparedStatement.executeUpdate();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void dropTable(String tableName) {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "DROP TABLE " + tableName + " CASCADE";
			statement.executeUpdate(sql);

			System.out.println("Tabela " + tableName + " excluída com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void createTable() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "CREATE TABLE cfop (\n" + "    id SERIAL PRIMARY KEY,\n" + "    codigo VARCHAR(10) NOT NULL,\n"
					+ "    descricao VARCHAR(255) NOT NULL,\n" + "    ativo BOOLEAN NOT NULL,\n"
					+ "    deletado BOOLEAN NOT NULL DEFAULT false\n" + ");";

			statement.executeUpdate(sql);

			System.out.println("Tabela CFOP criada com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaDeletado() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE cfop\n " + "ALTER COLUMN deletado SET DEFAULT false;";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'deletado' adicionada à tabela 'CFOP' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaIdAntigo() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE CFOP ADD idAntigo int";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'idAntigo' adicionada à tabela 'CFOP' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static int encontraCFOP(int idAntingo) throws SQLException {
		Connection conn = ConexaoNova.obterConexao();
		int id = 0;
		try {
			String sql = "SELECT id, sigla, descricao, ativo FROM CFOP WHERE idAntigo = ?";
			PreparedStatement preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, idAntingo);
			ResultSet resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				id = resultSet.getInt("id");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		conn.close();
		return id;

	}

	public static void main(String[] args) {
		dropTable("CFOP");
		createTable();
		adicionarColunaDeletado();
		adicionarColunaIdAntigo();
		migrarTabela();
	}
}
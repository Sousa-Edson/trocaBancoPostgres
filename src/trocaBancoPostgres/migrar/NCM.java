package trocaBancoPostgres.migrar;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import trocaBancoPostgres.conexao.ConexaoAntiga;
import trocaBancoPostgres.conexao.ConexaoNova;

public class NCM {
	public static void migrarTabela() {
		try (Connection remetenteConexao = ConexaoAntiga.obterConexao()) {

			String query = " SELECT TRIM(ncm_prod) AS ncm_prod\n"
					+ "FROM public.produto\n"
					+ "WHERE stprod = 1 AND ncm_prod IS NOT NULL AND TRIM(ncm_prod) <> ''\n"
					+ "GROUP BY TRIM(ncm_prod)\n"
					+ "ORDER BY TRIM(ncm_prod) ASC;\n"
					+ "";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {

					String codigo = resultSet.getString("ncm_prod");
					String descricao = resultSet.getString("ncm_prod");

					salvar(codigo, descricao);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void salvar(String codigo, String descricao) {
		try (Connection novaConexao = ConexaoNova.obterConexao()) {
			// INSERT INTO ncm (codigo, descricao, ativo) VALUES (?, ?, ?)
			String sql = "INSERT INTO NCM (codigo, descricao, ativo) VALUES (?, ?, true)";

			try (PreparedStatement preparedStatement = novaConexao.prepareStatement(sql)) {

				preparedStatement.setString(1, codigo);
				preparedStatement.setString(2, descricao);
				System.out.println("codigo::" + codigo + " descricao::" + descricao);
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

	public static void createTableNCM() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "CREATE TABLE ncm (\n" + "    id SERIAL PRIMARY KEY,\n" + "    codigo VARCHAR(10) NOT NULL,\n"
					+ "    descricao VARCHAR(255) NOT NULL,\n" + "    ativo BOOLEAN NOT NULL,\n"
					+ "    deletado BOOLEAN NOT NULL DEFAULT false\n" + ");";

			statement.executeUpdate(sql);

			System.out.println("Tabela NCM criada com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaDeletado() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE NCM ADD deletado BOOLEAN DEFAULT false";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'deletado' adicionada à tabela 'NCM' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaIdAntigo() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE NCM ADD idAntigo int";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'idAntigo' adicionada à tabela 'NCM' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static int encontraNCM(String codigo) throws SQLException {
		Connection conn = ConexaoNova.obterConexao();
		int id = 0;
		try {
			String sql = "SELECT id, codigo, descricao, ativo FROM NCM WHERE codigo = ?";
			PreparedStatement preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setString(1, codigo);
			ResultSet resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				id = resultSet.getInt("id");
				System.out.println("encontraNCM:::"+id);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		conn.close();
		return id;

	}

	public static void main(String[] args) {
		dropTable("NCM");
		createTableNCM();
//		adicionarColunaDeletado();
		adicionarColunaIdAntigo();
		migrarTabela();
		System.out.println("Finalizado");
	}
}

package trocaBancoPostgres.migrar;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import trocaBancoPostgres.conexao.ConexaoAntiga;
import trocaBancoPostgres.conexao.ConexaoNova;

public class Item {

	public static void migrarTabela() {
		try (Connection remetenteConexao = ConexaoAntiga.obterConexao()) {

			String query = "SELECT id_mov, id_prod_ent, data_mov, nota_mov, qtd_mov, qtd_prod, qtd_prod_ex, qtd_calc, qtd_calc_ex, valor_real, valor_moeda, destino_mov, complemento_mov,\n"
					+ "registro_mov, volume, usuario_mov, modo_mov, total_mov, sistema_mov, stmovimento, stsaldo\n"
					+ "FROM public.movprodutobase order by id_mov desc;";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {
					int id = resultSet.getInt("id_referencianatureza");
					String descricao = resultSet.getString("desc_natureza");
					System.out.println("" + descricao);
//					salvar(id, "", descricao);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void salvar(int idTransacao, int idProduto, String complemento, double quantidade, int tipo) {
		try (Connection novaConexao = ConexaoNova.obterConexao()) {
			String sql = "INSERT INTO item (transacao_id, produto_id, complemento, quantidade, tipo) VALUES (?, ?, ?, ?, ?)";

			try (PreparedStatement preparedStatement = novaConexao.prepareStatement(sql)) {

				preparedStatement.setInt(1, idTransacao);
				preparedStatement.setInt(2, idProduto);
				preparedStatement.setString(3, complemento);
				preparedStatement.setDouble(4, quantidade);
				preparedStatement.setInt(5, tipo);

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

			String sql = "CREATE TABLE item (\n" + "    id SERIAL PRIMARY KEY,\n"
					+ "    produto_id INT REFERENCES produto (id),\n" + "    complemento TEXT,\n"
					+ "    quantidade NUMERIC(10, 4) NOT NULL,\n" + "    tipo INTEGER NOT NULL,\n"
					+ "    transacao_id INTEGER NOT NULL\n" + ");";

			statement.executeUpdate(sql);

			System.out.println("Tabela Item criada com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaDeletado() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE item ADD COLUMN deletado BOOLEAN NOT NULL DEFAULT false;";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'deletado' adicionada à tabela 'Item' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

//	public static int encontraItem(int idAntingo) throws SQLException {
//		Connection conn = ConexaoNova.obterConexao();
//		int id = 0;
//		try {
//			String sql = "SELECT id, codigo, descricao, ativo FROM item WHERE idAntigo = ?";
//			PreparedStatement preparedStatement = conn.prepareStatement(sql);
//			preparedStatement.setInt(1, idAntingo);
//			ResultSet resultSet = preparedStatement.executeQuery();
//			if (resultSet.next()) {
//				id = resultSet.getInt("id");
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		conn.close();
//		return id;
//
//	}

	public static void main(String[] args) {
		dropTable("item");
		createTable();
		adicionarColunaDeletado();
		migrarTabela();
	}
}
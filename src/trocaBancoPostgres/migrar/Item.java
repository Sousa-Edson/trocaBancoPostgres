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
					+ "FROM public.movprodutobase where stmovimento=1 and nota_mov !=0 order by id_mov asc;";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {

					int idTransacao = resultSet.getInt("nota_mov");
					int idProduto = resultSet.getInt("id_prod_ent");
					String complemento = resultSet.getString("complemento_mov");
					double quantidade = resultSet.getDouble("qtd_mov");

					int tipo = 0;
					if (quantidade < 0) {
						tipo = 1;
					}

					quantidade = Math.abs(quantidade);

					System.out.println("idTransacao::" + idTransacao);
					idTransacao = Nota.encontraNota(idTransacao);
					
					
					if (idTransacao != 0) {
						System.out.println("nota" + (idTransacao));
						salvar(idTransacao, idProduto, complemento, quantidade, tipo);
						System.out.println("salvo ==" + tipo);
					}

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
			System.out.println("###SALVO AQUI " + idTransacao);
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

	public static void migrarTabelaNota(int idNotaAntigo) {
		System.out.println("Recebendo - migrarTabelaNota::" + idNotaAntigo);
		try (Connection remetenteConexao = ConexaoAntiga.obterConexao()) {

			String query = "SELECT id_mov, id_prod_ent, data_mov, nota_mov, qtd_mov, qtd_prod, qtd_prod_ex, qtd_calc, qtd_calc_ex, valor_real, valor_moeda, destino_mov, complemento_mov,\n"
					+ "registro_mov, volume, usuario_mov, modo_mov, total_mov, sistema_mov, stmovimento, stsaldo\n"
					+ "FROM public.movprodutobase where stmovimento=1 and nota_mov = " + idNotaAntigo
					+ " order by id_mov asc;";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {

					int idTransacao = resultSet.getInt("nota_mov");
					int idProduto = resultSet.getInt("id_prod_ent");
					String complemento = resultSet.getString("complemento_mov");
					double quantidade = resultSet.getDouble("qtd_mov");

					int tipo = 0;
					if (quantidade < 0) {
						tipo = 1;
					}

					quantidade = Math.abs(quantidade);

					System.out.println("idTransacao::" + idTransacao);
					idTransacao = Nota.encontraNota(idTransacao);
					
					idProduto = Produto.encontraProduto(idProduto);
					System.out.println("##idProduto::"+idProduto);
					
					if (idTransacao != 0) {
						System.out.println("nota" + (idTransacao));
						salvar(idTransacao, idProduto, complemento, quantidade, tipo);
						System.out.println("salvo ==" + tipo);
					}

				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		dropTable("item");
		createTable();
		adicionarColunaDeletado();
//		migrarTabela();
	}
}
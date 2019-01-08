import argparse
import mysql.connector
from MAIN import SENTI
import traceback

def updateAssessment(args):

    try:
        db = mysql.connector.connect(user=args.username, password=args.password, port=args.port, host=args.host, database=args.schema, charset='utf8')
        cur = db.cursor()
    except Exception as e:
        print(e)
    else:
        print('successfully connect to the databse')

    batch = 0
    while True:

        # get the (next batch of) candidates
        query_op = 'SELECT id, title, subline FROM {} WHERE negative is NULL ORDER BY id DESC LIMIT {}, {}'.format(args.table, batch * 100, (batch + 1) * 100)
        # query_op = 'SELECT id, title, subline FROM {} WHERE negative=-1'.format(table)
        cur.execute(query_op)
        list_of_news = cur.fetchall()
        print('got {} new lines '.format(len(list_of_news)))

        if len(list_of_news) <=0:
            break

        for line in list_of_news:

            try:
                id, title, subline = line

                # scoring title
                if title is not None:
                    sentiValueTitle = SENTI(title)
                    ReviewScoreTitle = sentiValueTitle.write2SentiValues()
                else:
                    ReviewScoreTitle = 0
                # scoring abstract
                if subline is not None:
                    sentiValueSubline = SENTI(subline)
                    ReviewScoreSubline = sentiValueSubline.write2SentiValues()
                else:
                    ReviewScoreSubline = 0

                # combine the title and abstract scores
                finalScore = ReviewScoreSubline + ReviewScoreTitle

                # transform the score into prediction
                if finalScore > 0:
                    prediction = 1
                elif finalScore == 0:
                    prediction = 0
                else:
                    prediction = 2
                print('Record #{} predicted to be {} with score {} ({}+{})'.format(id, prediction, finalScore, ReviewScoreSubline, ReviewScoreTitle))

                # update table with prediction
                update_op = 'UPDATE {} SET negative={} WHERE id={}'.format(args.table, prediction, id)
                cur.execute(update_op)
                db.commit()
                print('Successfully updated {}'.format(id))
                print("=======================")

            except Exception as ex:
                print(''.join(traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)))

    print('successfully updated all')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Set the parameters to connect to the database and update the assessment')
    parser.add_argument('--username', type=str, help='username to access the database')
    parser.add_argument('--password', type=str, help='password to access the database')
    parser.add_argument('--host', type=str, default='39.104.67.5', help='host to access the database')
    parser.add_argument('--port', type=str, default='3306', help='port to access the database')
    parser.add_argument('--schema', type=str, default='spider', help='schema to access the database')
    parser.add_argument('--table', type=str, default='rslist3', help='table to be updated')

    args = parser.parse_args()

    updateAssessment(args)
    print('successfully updated all')
